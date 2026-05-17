"""
glue_inject_raw_to_landing.py
------------------------------
AWS Glue ETL Script — Stage 1: INJECT
Reads raw data from a source S3 bucket, handles broken JSON CSV fragments,
ensures the Glue Catalog database exists, and writes to the landing
S3 bucket in Parquet format with Athena cataloging.

Partition column: load_timestamp  (format: YYYY-MM-DD-HH)
"""

import sys
import logging
import boto3
from botocore.exceptions import ClientError
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
    'source_format',       # json | csv | parquet
    'target_bucket',
    'target_prefix',
    'job_bookmark',        # enable | disable
    'log_level',
    'AWS_REGION'           # Optional: fallback to us-east-1 if not provided
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
# 2.  LOAD TIMESTAMP & PATHS
# ──────────────────────────────────────────────────────────────
RUN_TS         = datetime.now(timezone.utc)
LOAD_TIMESTAMP = RUN_TS.strftime('%Y-%m-%d-%H')
PARTITION_COL  = 'load_timestamp'
CATALOG_DB     = 'marketo_landing'
CATALOG_TABLE  = 'leads_raw'

def clean_bucket(val: str) -> str:
    """Removes stray s3:// or trailing slashes to prevent 400 Errors"""
    return val.replace('s3://', '').replace('s3:/', '').strip('/')

SOURCE_PATH = f"s3://{clean_bucket(args['source_bucket'])}/{args['source_prefix'].strip('/')}/"
TARGET_PATH = f"s3://{clean_bucket(args['target_bucket'])}/{args['target_prefix'].strip('/')}/"
SOURCE_FMT  = args['source_format'].lower()

log.info("=" * 64)
log.info(f"  JOB NAME      : {args['JOB_NAME']}")
log.info(f"  SOURCE PATH   : {SOURCE_PATH}  [{SOURCE_FMT}]")
log.info(f"  TARGET PATH   : {TARGET_PATH}  [parquet]")
log.info(f"  CATALOG       : {CATALOG_DB}.{CATALOG_TABLE}")
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
    raise ValueError(f"Unsupported source_format '{SOURCE_FMT}'.")

fmt_opts = FORMAT_OPTIONS_MAP[SOURCE_FMT]

source_dyf = glueContext.create_dynamic_frame.from_options(
    connection_type='s3',
    connection_options={
        'paths':      [SOURCE_PATH],
        'recurse':    True,
        'groupFiles': 'inPartition',
        'groupSize':  '134217728', # 128 MB per Spark partition
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
# 4.  RECONSTRUCT BROKEN JSON COLUMNS
# ──────────────────────────────────────────────────────────────
df = source_dyf.toDF()
log.info(f"[STEP 1b] Raw columns from reader: {len(df.columns)}")

JSON_CHARS = set('{', '}', '[', ']', ':', '"')

def is_json_fragment(col_name: str) -> bool:
    return any(ch in col_name for ch in JSON_CHARS)

broken_cols = [c for c in df.columns if is_json_fragment(c)]

if broken_cols:
    log.info(f"[STEP 1b] Broken JSON fragment columns detected ({len(broken_cols)}). Reconstructing...")

    json_rebuilt = ','.join(broken_cols)
    
    EVENT_SCHEMA = ArrayType(StructType([
        StructField("vin",             StringType(),  True),
        StructField("driver_id",       StringType(),  True),
        StructField("speed",           IntegerType(), True),
        StructField("lat",             DoubleType(),  True),
        StructField("long",            DoubleType(),  True),
        StructField("event_timestamp", StringType(),  True),
    ]))

    df = df.withColumn("_raw_json", F.lit(json_rebuilt))
    df = df.withColumn("_events",   F.from_json(F.col("_raw_json"), EVENT_SCHEMA))

    df = (df
          .withColumn("event_vin",        F.col("_events")[0]["vin"])
          .withColumn("event_driver_id",  F.col("_events")[0]["driver_id"])
          .withColumn("event_speed",      F.col("_events")[0]["speed"].cast(StringType()))
          .withColumn("event_lat",        F.col("_events")[0]["lat"].cast(StringType()))
          .withColumn("event_long",       F.col("_events")[0]["long"].cast(StringType()))
          .withColumn("event_timestamp",  F.col("_events")[0]["event_timestamp"])
         )

    df = df.drop(*broken_cols).drop("_raw_json", "_events")
    log.info("[STEP 1b] JSON reconstruction complete.")


# ──────────────────────────────────────────────────────────────
# 5.  ENRICHMENT
# ──────────────────────────────────────────────────────────────
log.info("[STEP 2] Enriching data — sanitising names, casting to string")

rename_map = {
    c: c.strip().lower().replace(' ', '_').replace('-', '_')
    for c in df.columns
}
for old, new in rename_map.items():
    if old != new:
        df = df.withColumnRenamed(old, new)

for col_name in df.columns:
    df = df.withColumn(col_name, df[col_name].cast(StringType()))

df = df.dropna(how='all')

df = (df
      .withColumn(PARTITION_COL,           F.lit(LOAD_TIMESTAMP))
      .withColumn('_load_timestamp_full',  F.lit(RUN_TS.strftime('%Y-%m-%dT%H:%M:%SZ')))
      .withColumn('_source_path',          F.lit(SOURCE_PATH))
      .withColumn('_source_format',        F.lit(SOURCE_FMT))
      .withColumn('_job_name',             F.lit(args['JOB_NAME']))
     )

enriched_count = df.count()
log.info(f"[STEP 2] Records after enrichment: {enriched_count:,}")


# ──────────────────────────────────────────────────────────────
# 6.  REPARTITION
# ──────────────────────────────────────────────────────────────
ROWS_PER_PARTITION = 65_536
num_partitions     = max(1, enriched_count // ROWS_PER_PARTITION)
log.info(f"[STEP 3] Repartitioning to {num_partitions} output file(s)")
df = df.repartition(num_partitions)


# ──────────────────────────────────────────────────────────────
# 7.  ENSURE DATABASE EXISTS (BULLETPROOFING)
# ──────────────────────────────────────────────────────────────
glue_region = args.get('AWS_REGION', 'us-east-1')
glue_client = boto3.client('glue', region_name=glue_region)

try:
    glue_client.get_database(Name=CATALOG_DB)
    log.info(f"[STEP 4] Database '{CATALOG_DB}' exists.")
except ClientError as e:
    if e.response['Error']['Code'] == 'EntityNotFoundException':
        log.info(f"[STEP 4] Database '{CATALOG_DB}' not found. Creating it now...")
        glue_client.create_database(
            DatabaseInput={
                'Name': CATALOG_DB,
                'Description': 'Automatically created by Glue Inject Job'
            }
        )
    else:
        log.error(f"[STEP 4] Failed to check/create database: {str(e)}")
        raise e


# ──────────────────────────────────────────────────────────────
# 8.  WRITE TO LANDING BUCKET & UPDATE CATALOG (TABLE)
# ──────────────────────────────────────────────────────────────
log.info(f"[STEP 5] Writing to S3 and registering in Athena catalog...")

landing_dyf = DynamicFrame.fromDF(df, glueContext, 'landing_dyf')

sink = glueContext.getSink(
    connection_type='s3',
    path=TARGET_PATH,
    enableUpdateCatalog=True,              # Automates table creation in Athena
    updateBehavior='UPDATE_IN_DATABASE',   # Automates schema updates in Athena
    partitionKeys=[PARTITION_COL],         # Automates partitions in Athena
    transformation_ctx='sink',
)
sink.setFormat('glueparquet', format_options={'compression': 'snappy'})
sink.setCatalogInfo(
    catalogDatabase=CATALOG_DB,
    catalogTableName=CATALOG_TABLE,
)
sink.writeFrame(landing_dyf)

log.info("[STEP 5] Write complete.")


# ──────────────────────────────────────────────────────────────
# 9.  SUMMARY
# ──────────────────────────────────────────────────────────────
log.info("=" * 64)
log.info("  INJECT SUMMARY")
log.info(f"  Source records read    : {record_count:>10,}")
log.info(f"  Records written        : {enriched_count:>10,}")
log.info(f"  Partition column       : {PARTITION_COL}")
log.info(f"  Partition value        : {LOAD_TIMESTAMP}")
log.info(f"  Target path            : {TARGET_PATH}{PARTITION_COL}={LOAD_TIMESTAMP}/")
log.info(f"  Catalog Destination    : {CATALOG_DB}.{CATALOG_TABLE}")
log.info("=" * 64)

job.commit()
log.info("[DONE] Job committed successfully.")