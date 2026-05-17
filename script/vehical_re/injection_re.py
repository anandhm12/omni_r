"""
glue_inject_raw_to_landing.py
------------------------------
AWS Glue ETL Script — Stage 1: INJECT
Reads raw data from a source S3 bucket and writes it
to the landing (injection) S3 bucket in Parquet format.

Partition column: load_timestamp  (format: YYYY-MM-DD-HH)

Fix applied:
  - col1 column contains embedded JSON string
    e.g. {"vin":"n901398e","driver_id":"drv_261","speed":112,
           "lat":30.445263,"long":-79.134266,"event_timestamp":"..."}
  - CSV was being read with JSON keys treated as column names
  - Fix: parse col1 as JSON → extract lat, long, speed, etc. as proper columns
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
    DoubleType, IntegerType
)

# ──────────────────────────────────────────────────────────────
# 1.  JOB INIT
# ──────────────────────────────────────────────────────────────
args = getResolvedOptions(sys.argv, [
    'JOB_NAME',
    'source_bucket',
    'source_prefix',
    'source_format',       # json | csv | parquet
    'target_bucket',
    'target_prefix',
    'job_bookmark',        # enable | disable
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
# 2.  LOAD TIMESTAMP  ← partition column
# ──────────────────────────────────────────────────────────────
RUN_TS         = datetime.now(timezone.utc)
LOAD_TIMESTAMP = RUN_TS.strftime('%Y-%m-%d-%H')
PARTITION_COL  = 'load_timestamp'

# ── Clean bucket helper (strips accidental s3:// prefix) ─────
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
    'json':    {
        'format': 'json',
        'format_options': {'multiLine': 'true'}
    },
    'csv':     {
        'format': 'csv',
        'format_options': {
            'withHeader': 'true',
            'separator':  ',',
            'quoteChar':  '"',
            'escaper':    '\\',      # handle escaped quotes inside JSON strings
        }
    },
    'parquet': {
        'format': 'parquet',
        'format_options': {}
    },
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

df = source_dyf.toDF().cache()
record_count = df.count()
log.info(f"[STEP 1] Records read from source : {record_count:,}")
log.info(f"[STEP 1] Columns detected         : {df.columns}")

if record_count == 0:
    log.warning("[STEP 1] No new records found — nothing to inject. Exiting.")
    job.commit()
    sys.exit(0)


# ──────────────────────────────────────────────────────────────
# 4.  FIX — PARSE JSON EMBEDDED IN col1
#
#     Problem:
#       CSV has a column called col1 that contains a raw JSON string:
#       {"vin":"n901398e","driver_id":"drv_261","speed":112,
#        "lat":30.445263,"long":-79.134266,"event_timestamp":"..."}
#
#       Spark treated the JSON keys as column names, causing:
#       AnalysisException: UNRESOLVED_COLUMN `"lat":_30`.`445263`
#
#     Fix:
#       1. Detect col1 (column containing JSON)
#       2. Parse it using from_json() with explicit schema
#       3. Expand parsed fields as proper DataFrame columns
#       4. Drop the raw JSON column
# ──────────────────────────────────────────────────────────────
JSON_COL = 'col1'   # column that holds the raw JSON string

# Schema matching the JSON structure inside col1
EVENT_JSON_SCHEMA = StructType([
    StructField("vin",             StringType(),  True),
    StructField("driver_id",       StringType(),  True),
    StructField("speed",           IntegerType(), True),
    StructField("lat",             DoubleType(),  True),
    StructField("long",            DoubleType(),  True),
    StructField("event_timestamp", StringType(),  True),
])

if JSON_COL in df.columns:
    log.info(f"[STEP 2a] Detected JSON column '{JSON_COL}' — parsing embedded JSON")

    # Parse the JSON string into a struct
    df = df.withColumn("_event_parsed", F.from_json(F.col(JSON_COL), EVENT_JSON_SCHEMA))

    # Expand struct fields into individual columns
    df = (df
          .withColumn("event_vin",        F.col("_event_parsed.vin"))
          .withColumn("event_driver_id",  F.col("_event_parsed.driver_id"))
          .withColumn("event_speed",      F.col("_event_parsed.speed").cast(StringType()))
          .withColumn("event_lat",        F.col("_event_parsed.lat").cast(StringType()))
          .withColumn("event_long",       F.col("_event_parsed.long").cast(StringType()))
          .withColumn("event_timestamp",  F.col("_event_parsed.event_timestamp"))
         )

    # Drop raw JSON column and temp struct
    df = df.drop("_event_parsed", JSON_COL)

    log.info("[STEP 2a] JSON fields extracted: "
             "event_vin, event_driver_id, event_speed, event_lat, event_long, event_timestamp")
else:
    log.info(f"[STEP 2a] Column '{JSON_COL}' not found — skipping JSON parse step")


# ──────────────────────────────────────────────────────────────
# 5.  ENRICHMENT
#     • Sanitise column names
#     • Cast all columns to string
#     • Drop fully-null rows
#     • Stamp audit + partition columns
# ──────────────────────────────────────────────────────────────
log.info("[STEP 2] Enriching data — sanitize + audit + partition column")

# Sanitise column names: lowercase, replace spaces/dashes with underscores
clean_cols = {
    c: c.strip().lower().replace(' ', '_').replace('-', '_')
    for c in df.columns
}
for old, new in clean_cols.items():
    if old != new:
        df = df.withColumnRenamed(old, new)

# Cast every column to string — schema-on-read at landing stage
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
log.info(f"[STEP 2] Final columns                : {df.columns}")

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
#
#     s3://<target_bucket>/<target_prefix>/
#         load_timestamp=2026-05-16-14/
#             part-00000.snappy.parquet
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