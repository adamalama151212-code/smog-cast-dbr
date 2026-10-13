#!/usr/bin/env bash
# Builds functions/gios_poller/dist/gios_poller.zip — the function code plus every Python package it needs,
# vendored in .python_packages/lib/site-packages (where the Functions host looks for them).
#
# Run it in the smogcast Docker image (Linux, Python 3.11 = the function's runtime, so binary wheels match):
#   docker compose run --rm smogcast bash functions/gios_poller/build.sh
# Deploy (Azure CLI on the laptop, logged in to the environment's subscription):
#   az functionapp deployment source config-zip -g <resource group> -n <function app> --src functions/gios_poller/dist/gios_poller.zip
set -euo pipefail

here="$(cd "$(dirname "$0")" && pwd)"
root="$(cd "$here/../.." && pwd)"
out="$here/dist"
stage="$out/stage"

rm -rf "$out"
mkdir -p "$stage" "$out/wheels"

# Our code: the same wheels the Databricks Jobs install (smogcast-core has no Spark dependency outside [local]).
for package in core ingest; do
    python -m build --wheel --outdir "$out/wheels" "$root/packages/$package" >/dev/null
done

pip install --quiet --no-cache-dir --target "$stage/.python_packages/lib/site-packages" \
    -r "$here/requirements.txt" "$out"/wheels/*.whl

cp "$here/function_app.py" "$here/host.json" "$here/requirements.txt" "$stage/"

python - "$stage" "$out/gios_poller" <<'PY'
import shutil, sys
shutil.make_archive(sys.argv[2], "zip", root_dir=sys.argv[1])
PY
rm -rf "$stage" "$out/wheels"
echo "built $out/gios_poller.zip"
