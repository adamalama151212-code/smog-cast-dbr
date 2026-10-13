"""Spark MLlib models: logistic regression (main, well-calibrated, explainable) and gradient
boosted trees (comparison). One model per pollutant; the city is a one-hot feature, so all
cities share what they have in common (weather → smog) while keeping their own base level.

Procedure (conf/model.yaml, fixed before seeing results):
1. for each grid point: fit on TRAIN years, score Brier on the VALIDATION year;
2. per model type keep the best grid point; the operational model of the pollutant is the
   model type with the lowest validation Brier;
3. refit the chosen settings on train + validation years → the "final" model, used for the
   test year and for live forecasts. The test year never influences any choice.
"""

from __future__ import annotations

import itertools
import json
import logging

from pyspark.ml import Pipeline, PipelineModel
from pyspark.ml.classification import GBTClassifier, LogisticRegression
from pyspark.ml.feature import OneHotEncoder, StringIndexer, VectorAssembler
from pyspark.ml.functions import vector_to_array
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from smogcast.model.prepare import NUMERIC_FEATURES

log = logging.getLogger(__name__)


def build_pipeline(model_type: str, params: dict, seed: int) -> Pipeline:
    stages = [
        StringIndexer(inputCol="city_id", outputCol="city_idx", handleInvalid="keep"),
        OneHotEncoder(inputCols=["city_idx"], outputCols=["city_vec"]),
        # handleInvalid="error": a NULL feature must fail loudly, not silently become NaN.
        VectorAssembler(inputCols=[*NUMERIC_FEATURES, "city_vec"], outputCol="features", handleInvalid="error"),
    ]
    if model_type == "logistic":
        # standardization=True (default): coefficients are comparable and regularisation is fair.
        stages.append(LogisticRegression(labelCol="label", featuresCol="features", maxIter=200,
                                         regParam=params["reg_param"], elasticNetParam=0.0))
    elif model_type == "gbt":
        stages.append(GBTClassifier(labelCol="label", featuresCol="features", seed=seed,
                                    maxDepth=params["max_depth"], maxIter=params["max_iter"], stepSize=0.1))
    else:
        raise ValueError(f"Unknown model type {model_type!r}")
    return Pipeline(stages=stages)


# A unique row order of the training data (one pollutant: one row per city and issue day).
FIT_ORDER = ["pollutant", "city_id", "issue_day"]


def fit_input(df: DataFrame) -> DataFrame:
    """The training rows in ONE partition and a fixed order, so a fit gives the same model everywhere.

    GBT samples rows per partition (with the seed) to choose its split thresholds, so the same data split
    differently — laptop vs Databricks cluster — grew different trees: on 2026-10-06 the validation Brier
    of the same GBT settings differed by up to 0.002 between Docker and DEV, and for PM2.5 that flipped the
    operational model (GBT locally, logistic on DEV). Logistic regression was identical to the last digit.
    A single partition costs nothing at this size (~10 thousand rows per pollutant).
    """
    return df.repartition(1).sortWithinPartitions(*FIT_ORDER)


def grid(spec: dict) -> list[dict]:
    keys = sorted(spec)
    return [dict(zip(keys, values, strict=True)) for values in itertools.product(*(spec[k] for k in keys))]


def predict(model: PipelineModel, df: DataFrame) -> DataFrame:
    """Probability of an exceedance tomorrow (class 1)."""
    out = model.transform(df)
    return out.withColumn("probability", vector_to_array("probability")[1]).drop(
        "city_idx", "city_vec", "features", "rawPrediction", "prediction"
    )


def brier(scored: DataFrame) -> float:
    return scored.agg(F.avg((F.col("probability") - F.col("label")) ** 2)).first()[0]


def tune(prepared: DataFrame, model_cfg: dict, seed: int) -> list[dict]:
    """Every grid point of every model type with its validation Brier (one pollutant)."""
    train = fit_input(prepared.where("split = 'train'")).cache()
    valid = prepared.where("split = 'validation'").cache()
    results = []
    for model_type in model_cfg["models"]:
        for params in grid(model_cfg["grids"][model_type]):
            model = build_pipeline(model_type, params, seed).fit(train)
            score = brier(predict(model, valid))
            log.info("tune %s %s: validation Brier %.5f", model_type, params, score)
            results.append({"model": model_type, "params": params, "validation_brier": score})
    train.unpersist()
    valid.unpersist()
    return results


def choose(results: list[dict]) -> tuple[dict[str, dict], str]:
    """Best grid point per model type, and the operational model type (lowest validation Brier)."""
    best: dict[str, dict] = {}
    for r in results:
        if r["model"] not in best or r["validation_brier"] < best[r["model"]]["validation_brier"]:
            best[r["model"]] = r
    operational = min(best.values(), key=lambda r: r["validation_brier"])["model"]
    return best, operational


def selection_rows(pollutant: str, results: list[dict], best: dict[str, dict], operational: str) -> list[tuple]:
    return [
        (pollutant, r["model"], json.dumps(r["params"], sort_keys=True), float(r["validation_brier"]),
         best[r["model"]] is r, r["model"] == operational and best[r["model"]] is r)
        for r in results
    ]


SELECTION_SCHEMA = ("pollutant STRING, model STRING, params STRING, validation_brier DOUBLE, "
                    "best_of_type BOOLEAN, operational BOOLEAN")
