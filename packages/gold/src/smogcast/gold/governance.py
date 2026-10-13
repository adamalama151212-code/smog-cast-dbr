"""Governance of the gold layer (ARCHITECTURE.md, section 4a): documented columns, row- and column-level security.

Everything is generated from one data dictionary (conf/gold_dictionary.yaml):
- column comments and ``class`` tags on the gold tables in Unity Catalog;
- one DYNAMIC VIEW per gold table in schema ``gold_secure`` that filters rows and masks columns by who is asking.

Why views and not row filters / column masks on the tables: a table with a filter or a mask can no longer be written
through the Delta Lake API from the Jobs' single-user clusters (only SQL ``MERGE INTO`` on DBR 16.3+). The views leave
the gold tables and the Jobs untouched; analysts and regional users get SELECT on ``gold_secure`` only.

Who is asking → role (``viewer`` CTE of every view):
1. a row for ``current_user()`` in ``ops.access_policy`` (role admin / analyst / region + voivodeship) — works where
   account groups cannot be managed (the course's PROD) and makes the demo a one-line UPDATE of one's own row;
2. otherwise account-group membership (Terraform module governance): smogcast-admins, smogcast-analysts,
   smogcast-region-<code>;
3. otherwise ``none`` — no rows at all.

Rules: admin sees everything; analyst every row with ``technical`` columns masked (NULL); region only the rows of its
voivodeship(s) (+ aggregate rows city_id = 'ALL'), technical columns masked. Pure functions: statements are strings.
"""

from __future__ import annotations

ADMIN, ANALYST, REGION, NONE = "admin", "analyst", "region", "none"
TECHNICAL = "technical"


def lit(text: str) -> str:
    """A SQL string literal."""
    return "'" + str(text).replace("\\", "\\\\").replace("'", "\\'") + "'"


def ident(name: str) -> str:
    return f"`{name}`"


def check_columns(table: str, entry: dict, columns: list[str]) -> list[str]:
    """Undocumented or badly documented columns of ``table`` (empty = fine).

    A documented column the table does not have YET is not a problem: tables evolve forward (e.g. run_kind reaches
    gold.forecast_tomorrow with the first forecast run of the new code); the views use the table's real columns.
    """
    documented = set(entry["columns"])
    problems = [f"gold.{table}.{c}: column not in conf/gold_dictionary.yaml" for c in columns if c not in documented]
    for c, spec in entry["columns"].items():
        if spec.get("class") not in ("business", TECHNICAL) or not spec.get("description"):
            problems.append(f"gold.{table}.{c}: needs class business|technical and a description")
    return problems


def comment_statements(fq_table: str, entry: dict, columns: list[str], tags: bool = True) -> list[str]:
    """Table and column comments (+ ``class`` tags) for a gold table."""
    out = [f"COMMENT ON TABLE {fq_table} IS {lit(entry['description'])}"]
    for c in columns:
        spec = entry["columns"][c]
        out.append(f"ALTER TABLE {fq_table} ALTER COLUMN {ident(c)} COMMENT {lit(spec['description'])}")
        if tags:
            out.append(f"ALTER TABLE {fq_table} ALTER COLUMN {ident(c)} SET TAGS ('class' = {lit(spec['class'])})")
    return out


def _group_regions(cfg: dict) -> str:
    g = cfg["governance"]["groups"]
    codes = sorted({c["jurisdiction_code"] for c in cfg["cities"].values()})
    cases = ", ".join(f"CASE WHEN is_account_group_member({lit(g['region_prefix'] + code)}) THEN {lit(code)} END"
                      for code in codes)
    return f"filter(array({cases}), x -> x IS NOT NULL)"


def viewer_cte(cfg: dict, access_table: str) -> str:
    """CTE ``viewer(role, regions)`` for the person running the query."""
    g = cfg["governance"]["groups"]
    mine = f"FROM {access_table} WHERE principal = current_user()"
    from_table = (f"(SELECT CASE WHEN count(*) = 0 THEN NULL "
                  f"WHEN array_contains(collect_set(role), '{ADMIN}') THEN '{ADMIN}' "
                  f"WHEN array_contains(collect_set(role), '{ANALYST}') THEN '{ANALYST}' "
                  f"ELSE '{REGION}' END {mine})")
    group_regions = _group_regions(cfg)
    from_groups = (f"CASE WHEN is_account_group_member({lit(g['admin'])}) THEN '{ADMIN}' "
                   f"WHEN is_account_group_member({lit(g['analyst'])}) THEN '{ANALYST}' "
                   f"WHEN size({group_regions}) > 0 THEN '{REGION}' ELSE '{NONE}' END")
    regions = (f"CASE WHEN (SELECT count(*) {mine}) > 0 "
               f"THEN (SELECT collect_set(region) {mine} AND region IS NOT NULL) ELSE {group_regions} END")
    return f"viewer AS (SELECT COALESCE({from_table}, {from_groups}) AS role, {regions} AS regions)"


def _region_expr(cfg: dict, region_column: str) -> str:
    if region_column == "jurisdiction_code":
        return "t.jurisdiction_code"
    whens = " ".join(f"WHEN {lit(cid)} THEN {lit(c['jurisdiction_code'])}" for cid, c in sorted(cfg["cities"].items()))
    return f"CASE t.{ident(region_column)} {whens} END"


def secure_view(cfg: dict, fq_view: str, fq_table: str, access_table: str, entry: dict, columns: list[str]) -> str:
    """CREATE OR REPLACE VIEW: rows filtered and technical columns masked for the viewer's role."""
    col_list = ",\n  ".join(f"{ident(c)} COMMENT {lit(entry['columns'][c]['description'])}" for c in columns)
    return (f"CREATE OR REPLACE VIEW {fq_view} (\n  {col_list}\n) COMMENT {lit(entry['description'])} AS\n"
            + secure_select(cfg, fq_table, access_table, entry, columns))


def secure_select(cfg: dict, fq_table: str, access_table: str, entry: dict, columns: list[str]) -> str:
    """The query of a secure view (separate, so tests can run it on local Spark)."""
    select = ",\n  ".join(
        f"CASE WHEN v.role = '{ADMIN}' THEN t.{ident(c)} END AS {ident(c)}"
        if entry["columns"][c]["class"] == TECHNICAL else f"t.{ident(c)}" for c in columns)
    region_column = entry.get("region_column")
    if region_column:
        all_rows = (f"t.city_id = {lit(cfg['governance']['all_cities_id'])} OR " if "city_id" in columns else "")
        where = (f"v.role IN ('{ADMIN}', '{ANALYST}') OR (v.role = '{REGION}' AND "
                 f"({all_rows}array_contains(v.regions, {_region_expr(cfg, region_column)})))")
    else:
        where = f"v.role <> '{NONE}'"   # no per-city data: every known role sees it
    return (f"WITH {viewer_cte(cfg, access_table)}\n"
            f"SELECT\n  {select}\nFROM {fq_table} AS t CROSS JOIN viewer AS v\nWHERE {where}")


def access_table_statements(access_table: str, seed: list[dict], editors: list[str] = ()) -> list[str]:
    """The access table (if missing), its first rows and who may edit it — a principal that already has a row is left
    alone, so a role changed for a demo survives the next Job run.

    ``editors`` (groups or users) get write access to THIS table only: it belongs to the Jobs' principal like every
    other table, but roles are managed by people. The gold tables stay read-only for them.
    """
    out = [f"CREATE TABLE IF NOT EXISTS {access_table} (principal STRING NOT NULL, role STRING NOT NULL, region STRING, "
           f"note STRING, updated_at TIMESTAMP) COMMENT {lit('Who sees what in gold_secure: e-mail → admin / analyst / region (+ voivodeship code). Edited by admins only.')}"]
    out += [f"GRANT SELECT, MODIFY ON TABLE {access_table} TO {ident(principal)}" for principal in editors]
    for row in seed:
        region = lit(row["region"]) if row.get("region") else "NULL"
        out.append(f"INSERT INTO {access_table} SELECT {lit(row['principal'])}, {lit(row['role'])}, {region}, "
                   f"{lit(row.get('note', 'seed'))}, current_timestamp() "
                   f"WHERE NOT EXISTS (SELECT 1 FROM {access_table} WHERE principal = {lit(row['principal'])})")
    return out
