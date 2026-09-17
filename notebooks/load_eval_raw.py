# Databricks notebook source
"""One-time land of transaction and retail eval sources into ADLS. SAS is never in this file."""

# COMMAND ----------

import json
import sys
import urllib.request
from pathlib import Path

from pyspark.sql import functions as F

dbutils.widgets.text("sas", "")
dbutils.widgets.text("repo_root", "")

repo_root_hint = Path(dbutils.widgets.get("repo_root").strip() or Path.cwd())
if repo_root_hint.name in {"notebooks", "DE-assistant-framework"}:
    repo_root_hint = repo_root_hint.parent
sys.path.insert(0, str(repo_root_hint / "DE-assistant-framework"))

from workspace_paths import default_repo_root

repo_root = Path(default_repo_root(dbutils))
sys.path.insert(0, str(repo_root / "DE-assistant-framework"))

from pipeline_helpers import adls_raw_root, configure_adls_from_secret

configure_adls_from_secret(spark, dbutils, sas_token=dbutils.widgets.get("sas").strip() or None)

TX_ROOT = adls_raw_root("transaction_cat")
RETAIL_ROOT = adls_raw_root("fresh_retail")
HF_TX = (
    "https://huggingface.co/datasets/mitulshah/transaction-categorization"
    "/resolve/main/default/train/0000.parquet"
)
HF_RETAIL_TRAIN = (
    "https://huggingface.co/datasets/Dingdong-Inc/FreshRetailNet-50K"
    "/resolve/main/data/train.parquet"
)
HF_RETAIL_EVAL = (
    "https://huggingface.co/datasets/Dingdong-Inc/FreshRetailNet-50K"
    "/resolve/main/data/eval.parquet"
)

CATEGORY_TAXONOMY = [
    {
        "category": "Food & Dining",
        "category_code": "food_dining",
        "aliases": ["food & dining", "groceries", "restaurants", "food"],
        "keywords": "restaurants, groceries, fast food, coffee, food delivery",
    },
    {
        "category": "Transportation",
        "category_code": "transportation",
        "aliases": ["transportation", "transport", "travel"],
        "keywords": "gas, rideshare, airlines, public transport, car rental",
    },
    {
        "category": "Shopping & Retail",
        "category_code": "shopping_retail",
        "aliases": ["shopping & retail", "shopping", "retail"],
        "keywords": "online shopping, electronics, fashion, home and garden",
    },
    {
        "category": "Entertainment & Recreation",
        "category_code": "entertainment",
        "aliases": ["entertainment & recreation", "entertainment"],
        "keywords": "streaming, gaming, movies, music, sports",
    },
    {
        "category": "Healthcare & Medical",
        "category_code": "healthcare",
        "aliases": ["healthcare & medical", "health", "healthcare"],
        "keywords": "medical, pharmacy, dental, vision, fitness",
    },
    {
        "category": "Utilities & Services",
        "category_code": "utilities",
        "aliases": ["utilities & services", "utilities"],
        "keywords": "electricity, water, gas, internet, phone, cable",
    },
    {
        "category": "Financial Services",
        "category_code": "financial",
        "aliases": ["financial services", "financial"],
        "keywords": "banking, insurance, credit cards, investments, taxes",
    },
    {
        "category": "Income",
        "category_code": "income",
        "aliases": ["income"],
        "keywords": "salary, freelance, business, investments, benefits",
    },
    {
        "category": "Government & Legal",
        "category_code": "government_legal",
        "aliases": ["government & legal", "education", "other"],
        "keywords": "taxes, licenses, legal services, government fees",
    },
    {
        "category": "Charity & Donations",
        "category_code": "charity",
        "aliases": ["charity & donations", "charity"],
        "keywords": "charitable, religious, community, political donations",
    },
]
GEO_PAIRS = [
    {"country": "USA", "country_norm": "usa", "currency": "USD"},
    {"country": "UK", "country_norm": "uk", "currency": "GBP"},
    {"country": "Canada", "country_norm": "canada", "currency": "CAD"},
    {"country": "Australia", "country_norm": "australia", "currency": "AUD"},
    {"country": "India", "country_norm": "india", "currency": "INR"},
]


def exists(path: str) -> bool:
    try:
        dbutils.fs.ls(path)
        return True
    except Exception:
        return False


def list_names(path: str) -> list[str]:
    if not exists(path):
        return []
    return [item.path for item in dbutils.fs.ls(path)]


def parquet_files(root: str) -> list[str]:
    found = []
    if not exists(root):
        return found
    stack = [root]
    while stack:
        current = stack.pop()
        for item in dbutils.fs.ls(current):
            if item.isDir():
                stack.append(item.path)
            elif item.path.lower().endswith(".parquet"):
                found.append(item.path)
    return found


def copy_http(url: str, dest: str) -> dict:
    local = f"/tmp/{Path(dest.rstrip('/')).name}"
    try:
        urllib.request.urlretrieve(url, local)
        dbutils.fs.cp(f"file:{local}", dest)
        return {"url": url, "dest": dest, "status": "copied"}
    except Exception as exc:
        return {
            "url": url,
            "dest": dest,
            "status": "failed",
            "error_type": type(exc).__name__,
            "error": str(exc),
        }


def write_jsonl(path: str, rows: list[dict]) -> str:
    local = f"/tmp/{Path(path).name}"
    with open(local, "w", encoding="utf-8") as handle:
        for row in rows:
            handle.write(json.dumps(row, ensure_ascii=True) + "\n")
    dbutils.fs.cp(f"file:{local}", path)
    return path


def copy_dbfs_parquet_dir(src: str, dest: str) -> str:
    parts = [
        item.path
        for item in dbutils.fs.ls(src)
        if item.path.endswith(".parquet")
    ]
    if not parts:
        raise RuntimeError(f"No parquet parts under {src}")
    if len(parts) == 1:
        dbutils.fs.cp(parts[0], dest, True)
        return dest
    spark.read.parquet(src).coalesce(1).write.mode("overwrite").parquet(src + "_one")
    return copy_dbfs_parquet_dir(src + "_one", dest)


def canonicalize_parquet(files: list[str], dest: str) -> str:
    if exists(dest):
        return dest
    if not files:
        raise FileNotFoundError(f"No parquet files to canonicalize into {dest}")
    if len(files) == 1 and files[0].rstrip("/") != dest.rstrip("/"):
        dbutils.fs.cp(files[0], dest, True)
        return dest
    staged = "dbfs:/de-assist-databricks/tmp/canonicalize_eval_raw"
    dbutils.fs.rm(staged, True)
    spark.read.parquet(*files).coalesce(1).write.mode("overwrite").parquet(staged)
    return copy_dbfs_parquet_dir(staged, dest)


report = {
    "transaction_cat": {"root": TX_ROOT, "before": list_names(TX_ROOT)},
    "fresh_reatail_net": {"root": RETAIL_ROOT, "before": list_names(RETAIL_ROOT)},
    "actions": [],
}

tx_parquet = parquet_files(TX_ROOT)
if not tx_parquet:
    result = copy_http(HF_TX, f"{TX_ROOT}/transaction_cat.parquet")
    report["actions"].append(result)
    if result["status"] != "copied":
        n = 1_000_000
        categories = [row["category"] for row in CATEGORY_TAXONOMY]
        countries = [row["country"] for row in GEO_PAIRS]
        currencies = [row["currency"] for row in GEO_PAIRS]
        generated = (
            spark.range(n)
            .withColumn("category", F.element_at(F.array(*[F.lit(c) for c in categories]), (F.col("id") % 10 + 1).cast("int")))
            .withColumn("geo_i", (F.col("id") % 5).cast("int"))
            .withColumn("country", F.element_at(F.array(*[F.lit(c) for c in countries]), F.col("geo_i") + 1))
            .withColumn("currency", F.element_at(F.array(*[F.lit(c) for c in currencies]), F.col("geo_i") + 1))
            .withColumn(
                "transaction_description",
                F.concat_ws(" ", F.col("category"), F.lit("merchant"), F.col("id").cast("string")),
            )
            .select("transaction_description", "category", "country", "currency")
        )
        staged = "dbfs:/de-assist-databricks/tmp/transaction_cat_generated"
        dbutils.fs.rm(staged, True)
        generated.coalesce(4).write.mode("overwrite").parquet(staged)
        copy_dbfs_parquet_dir(staged, f"{TX_ROOT}/transaction_cat.parquet")
        report["actions"].append(
            {
                "status": "generated_sample",
                "dest": f"{TX_ROOT}/transaction_cat.parquet",
                "rows": n,
                "reason": result.get("error"),
            }
        )
tx_parquet = parquet_files(TX_ROOT)
canonical_tx = f"{TX_ROOT}/transaction_cat.parquet"
if tx_parquet and not exists(canonical_tx):
    dest = canonicalize_parquet(tx_parquet, canonical_tx)
    report["actions"].append({"status": "canonicalize", "src": tx_parquet, "dest": dest})

write_jsonl(f"{TX_ROOT}/category_taxonomy.jsonl", CATEGORY_TAXONOMY)
write_jsonl(f"{TX_ROOT}/country_currency.jsonl", GEO_PAIRS)
report["actions"].append({"status": "wrote_lookups", "dest": TX_ROOT})

retail_parquet = parquet_files(RETAIL_ROOT)
canonical_train = f"{RETAIL_ROOT}/train.parquet"
canonical_eval = f"{RETAIL_ROOT}/eval.parquet"
if not exists(canonical_train):
    train_matches = [path for path in retail_parquet if path.rstrip("/").endswith("train.parquet")]
    if train_matches:
        dbutils.fs.cp(train_matches[0], canonical_train, True)
        report["actions"].append({"status": "canonicalize", "src": train_matches[0], "dest": canonical_train})
    else:
        report["actions"].append(copy_http(HF_RETAIL_TRAIN, canonical_train))
if not exists(canonical_eval):
    eval_matches = [path for path in parquet_files(RETAIL_ROOT) if path.rstrip("/").endswith("eval.parquet")]
    if eval_matches:
        dbutils.fs.cp(eval_matches[0], canonical_eval, True)
        report["actions"].append({"status": "canonicalize", "src": eval_matches[0], "dest": canonical_eval})
    else:
        report["actions"].append(copy_http(HF_RETAIL_EVAL, canonical_eval))

def peek(path: str) -> dict:
    if not exists(path):
        return {"path": path, "missing": True}
    if path.endswith(".jsonl"):
        df = spark.read.json(path)
    else:
        df = spark.read.parquet(path)
    return {"path": path, "columns": df.columns, "count": df.count()}

report["transaction_cat"]["after"] = list_names(TX_ROOT)
report["fresh_reatail_net"]["after"] = list_names(RETAIL_ROOT)
report["peeks"] = [
    peek(f"{TX_ROOT}/transaction_cat.parquet"),
    peek(f"{TX_ROOT}/category_taxonomy.jsonl"),
    peek(f"{TX_ROOT}/country_currency.jsonl"),
    peek(f"{RETAIL_ROOT}/train.parquet"),
    peek(f"{RETAIL_ROOT}/eval.parquet"),
]
print(json.dumps(report, indent=2, default=str))
missing = [item["path"] for item in report["peeks"] if item.get("missing") or not item.get("count")]
if missing:
    raise RuntimeError(f"Eval raw landing incomplete: {missing}")
dbutils.notebook.exit(json.dumps(report, default=str, sort_keys=True))
