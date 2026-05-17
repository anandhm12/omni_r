"""
glue_inject_raw_to_landing.py
------------------------------
AWS Glue ETL Script — Stage 1: INJECT

ROOT CAUSE OF ERROR (line 148):
-------------------------------
Your CSV has an unquoted JSON column, for example:

  transaction_id,vin,...,[{"vin":"n901398e","lat":30.445263,"long":-79.134266,...}],driver_id,...

The CSV parser splits every comma — including commas INSIDE the JSON —
creating broken column names like:
  `[`  `{"vin":_"n901398e"`  `"lat":_30`.`445263`  etc.

Line 148 (the cast loop) then fails trying to reference those broken names.

FIX STRATEGY:
  After toDF(), BEFORE the cast loop:
    1. Identify all broken JSON-fragment columns (contain { } : [ ] chars)
    2. Concatenate them back into one JSON string
    3. Parse the JSON → extract lat, long, speed, etc. as proper columns
    4. Drop all broken fragment columns
    5. Continue normally with the cast loop
"""

import sys
import logging
from datetime import datetime, timezone

from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame

from pyspark.context import SparkContext
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StringType, StructType, StructField,
    DoubleType, IntegerType, ArrayType
)

# ──────────────────────────────────────────────────────────────
# 1.  JOB INIT
# ──────────────────────────────────────────────────────────────
args = getResolvedOptions(sys.argv, [
    'JOB_NAME',
    'source_bucket',
    'source_prefix',
    'source_format',
    'target_bucket',
    'target_prefix',
    'job_bookmark',
    'log_level',
])

sc          = SparkContext()
glueContext = GlueContext(sc)
spark       = glueContext.spark_session
job         = Job(glueContext)
job.init(args['JOB_NAME'], args)

# ── Logging ───────────────────────────────────────────────────
try:
    log_level_str = args['log_level'].upper()
except KeyError:
    log_level_str = 'INFO'

log_level = getattr(logging, log_level_str, logging.INFO)
logging.basicConfig(
    level=log_level,
    format='%(asctime)s  %(levelname)-8s  %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
)
log = logging.getLogger(__name__)

# ──────────────────────────────────────────────────────────────
# 2.  LOAD TIMESTAMP
# ──────────────────────────────────────────────────────────────
RUN_TS         = datetime.now(timezone.utc)
LOAD_TIMESTAMP = RUN_TS.strftime('%Y-%m-%d-%H')
PARTITION_COL  = 'load_timestamp'

def clean_bucket(val: str) -> str:
    return val.replace('s3://', '').strip('/')

SOURCE_PATH = f"s3://{clean_bucket(args['source_bucket'])}/{args['source_prefix'].strip('/')}/"
TARGET_PATH = f"s3://{clean_bucket(args['target_bucket'])}/{args['target_prefix'].strip('/')}/"
SOURCE_FMT  = args['source_format'].lower()

log.info("=" * 64)
log.info(f"  JOB NAME      : {args['JOB_NAME']}")
log.info(f"  SOURCE PATH   : {SOURCE_PATH}  [{SOURCE_FMT}]")
log.info(f"  TARGET PATH   : {TARGET_PATH}  [parquet]")
log.info(f"  PARTITION COL : {PARTITION_COL}")
log.info(f"  LOAD TIMESTAMP: {LOAD_TIMESTAMP}")
log.info(f"  JOB BOOKMARK  : {args['job_bookmark']}")
log.info("=" * 64)


# ──────────────────────────────────────────────────────────────
# 3.  READ SOURCE DATA
# ──────────────────────────────────────────────────────────────
log.info(f"[STEP 1] Reading source data from: {SOURCE_PATH}")

FORMAT_OPTIONS_MAP = {
    'json':    {'format': 'json',    'format_options': {'multiLine': 'true'}},
    'csv':     {'format': 'csv',     'format_options': {'withHeader': 'true',
                                                        'separator':  ',',
                                                        'quoteChar':  '"'}},
    'parquet': {'format': 'parquet', 'format_options': {}},
}

if SOURCE_FMT not in FORMAT_OPTIONS_MAP:
    raise ValueError(
        f"Unsupported source_format '{SOURCE_FMT}'. "
        f"Must be one of: {list(FORMAT_OPTIONS_MAP.keys())}"
    )

fmt_opts = FORMAT_OPTIONS_MAP[SOURCE_FMT]

source_dyf = glueContext.create_dynamic_frame.from_options(
    connection_type='s3',
    connection_options={
        'paths':      [SOURCE_PATH],
        'recurse':    True,
        'groupFiles': 'inPartition',
        'groupSize':  '134217728',
    },
    format=fmt_opts['format'],
    format_options=fmt_opts['format_options'],
    transformation_ctx='source_dyf',
)

record_count = source_dyf.count()
log.info(f"[STEP 1] Records read from source: {record_count:,}")

if record_count == 0:
    log.warning("[STEP 1] No new records found — nothing to inject. Exiting.")
    job.commit()
    sys.exit(0)


# ──────────────────────────────────────────────────────────────
# 4.  RECONSTRUCT BROKEN JSON COLUMNS  ← THE FIX FOR LINE 148
#
#     When a CSV has unquoted JSON like:
#       ..., [{"vin":"abc","lat":30.44,"long":-79.13,...}], ...
#
#     The CSV parser creates broken columns:
#       `[`  `{"vin":"abc"`  `"lat":30`  `.44`  `"long":-79` `.13` ...
#
#     We detect these broken fragments, stitch them back into a
#     single JSON string, parse it, then drop the fragments.
#
#     This runs BEFORE the cast loop (original line 148) so no
#     AnalysisException can occur.
# ──────────────────────────────────────────────────────────────
df = source_dyf.toDF()

log.info(f"[STEP 1b] Raw columns from CSV reader : {df.columns}")

# ── Helper: detect broken JSON fragment column names ─────────
JSON_CHARS = set('{', '}', '[', ']', ':', '"')

def is_json_fragment(col_name: str) -> bool:
    """True if column name contains JSON punctuation — i.e. a broken fragment."""
    return any(ch in col_name for ch in JSON_CHARS)

broken_cols = [c for c in df.columns if is_json_fragment(c)]
clean_cols_list = [c for c in df.columns if not is_json_fragment(c)]

if broken_cols:
    log.info(f"[STEP 1b] Broken JSON fragment columns detected ({len(broken_cols)}): {broken_cols}")
    log.info(f"[STEP 1b] Reconstructing JSON from fragments...")

    # ── Stitch fragments back into one JSON string per row ───
    # Each fragment column name IS the data (the CSV reader used
    # the value as the column name because it was in the header row
    # position after the split).
    # We concat all fragment names with commas to rebuild the JSON.
    json_rebuilt = ','.join(broken_cols)   # e.g. '[{"vin":"n901398e","lat":30.445263,...}]'
    log.info(f"[STEP 1b] Reconstructed JSON string  : {json_rebuilt[:120]}...")

    # ── Parse the reconstructed JSON string ──────────────────
    EVENT_SCHEMA = ArrayType(StructType([
        StructField("vin",             StringType(),  True),
        StructField("driver_id",       StringType(),  True),
        StructField("speed",           IntegerType(), True),
        StructField("lat",             DoubleType(),  True),
        StructField("long",            DoubleType(),  True),
        StructField("event_timestamp", StringType(),  True),
    ]))

    # Add reconstructed JSON as a literal column, parse it
    df = df.withColumn("_raw_json", F.lit(json_rebuilt))
    df = df.withColumn("_events",   F.from_json(F.col("_raw_json"), EVENT_SCHEMA))

    # Extract fields from first array element
    df = (df
          .withColumn("event_vin",        F.col("_events")[0]["vin"])
          .withColumn("event_driver_id",  F.col("_events")[0]["driver_id"])
          .withColumn("event_speed",      F.col("_events")[0]["speed"].cast(StringType()))
          .withColumn("event_lat",        F.col("_events")[0]["lat"].cast(StringType()))
          .withColumn("event_long",       F.col("_events")[0]["long"].cast(StringType()))
          .withColumn("event_timestamp",  F.col("_events")[0]["event_timestamp"])
         )

    # Drop broken fragment columns and temp columns
    df = df.drop(*broken_cols).drop("_raw_json", "_events")

    log.info("[STEP 1b] JSON reconstruction complete.")
    log.info(f"[STEP 1b] Clean columns now: {df.columns}")

else:
    log.info("[STEP 1b] No broken JSON fragment columns detected — skipping reconstruction")


# ──────────────────────────────────────────────────────────────
# 5.  ENRICHMENT
#     Now safe to sanitise names and cast — no broken cols left
# ──────────────────────────────────────────────────────────────
log.info("[STEP 2] Enriching data — audit columns + load_timestamp partition")

# Sanitise column names: lowercase, replace spaces/dashes with underscores
rename_map = {
    c: c.strip().lower().replace(' ', '_').replace('-', '_')
    for c in df.columns
}
for old, new in rename_map.items():
    if old != new:
        df = df.withColumnRenamed(old, new)

# Cast every column to string — schema-on-read at landing stage
# THIS IS THE ORIGINAL LINE 148 — now safe because broken cols are gone
for col_name in df.columns:
    df = df.withColumn(col_name, df[col_name].cast(StringType()))

# Drop rows where every field is null
df = df.dropna(how='all')

# Audit + partition columns
df = (df
      .withColumn(PARTITION_COL,           F.lit(LOAD_TIMESTAMP))
      .withColumn('_load_timestamp_full',  F.lit(RUN_TS.strftime('%Y-%m-%dT%H:%M:%SZ')))
      .withColumn('_source_path',          F.lit(SOURCE_PATH))
      .withColumn('_source_format',        F.lit(SOURCE_FMT))
      .withColumn('_job_name',             F.lit(args['JOB_NAME']))
     )

enriched_count = df.count()
log.info(f"[STEP 2] Records after null-row drop  : {enriched_count:,}")
log.info(f"[STEP 2] load_timestamp value applied : {LOAD_TIMESTAMP}")

if log_level == logging.DEBUG:
    df.printSchema()
    df.show(5, truncate=False)


# ──────────────────────────────────────────────────────────────
# 6.  REPARTITION
# ──────────────────────────────────────────────────────────────
ROWS_PER_PARTITION = 65_536
num_partitions     = max(1, enriched_count // ROWS_PER_PARTITION)
log.info(f"[STEP 3] Repartitioning to {num_partitions} output file(s)")
df = df.repartition(num_partitions)


# ──────────────────────────────────────────────────────────────
# 7.  WRITE TO LANDING BUCKET
# ──────────────────────────────────────────────────────────────
log.info(f"[STEP 4] Writing to   : {TARGET_PATH}")
log.info(f"         Partition key : {PARTITION_COL}={LOAD_TIMESTAMP}")

landing_dyf = DynamicFrame.fromDF(df, glueContext, 'landing_dyf')

sink = glueContext.getSink(
    connection_type='s3',
    path=TARGET_PATH,
    enableUpdateCatalog=True,
    updateBehavior='UPDATE_IN_DATABASE',
    partitionKeys=[PARTITION_COL],
    transformation_ctx='sink',
)
sink.setFormat('glueparquet', format_options={'compression': 'snappy'})
sink.setCatalogInfo(
    catalogDatabase='marketo_landing',
    catalogTableName='leads_raw',
)
sink.writeFrame(landing_dyf)

log.info("[STEP 4] Write complete.")


# ──────────────────────────────────────────────────────────────
# 8.  SUMMARY
# ──────────────────────────────────────────────────────────────
log.info("=" * 64)
log.info("  INJECT SUMMARY")
log.info(f"  Source records read    : {record_count:>10,}")
log.info(f"  Records written        : {enriched_count:>10,}")
log.info(f"  Dropped (null rows)    : {record_count - enriched_count:>10,}")
log.info(f"  Partition column       : {PARTITION_COL}")
log.info(f"  Partition value        : {LOAD_TIMESTAMP}")
log.info(f"  Full run timestamp     : {RUN_TS.strftime('%Y-%m-%dT%H:%M:%SZ')}")
log.info(f"  Target path            : {TARGET_PATH}{PARTITION_COL}={LOAD_TIMESTAMP}/")
log.info(f"  Output format          : parquet / snappy")
log.info("=" * 64)

job.commit()
log.info("[DONE] Job committed successfully.")