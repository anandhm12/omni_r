"""
glue_inject_raw_to_landing.py
------------------------------
AWS Glue ETL Script — Stage 1: INJECT
Reads raw data from a source S3 bucket and writes it
to the landing (injection) S3 bucket in Parquet format.

Partition column: load_timestamp  (format: YYYY-MM-DD-HH)
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
from pyspark.sql.types import StringType

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
log_level = getattr(logging, args.get('log_level', 'INFO').upper(), logging.INFO)
logging.basicConfig(
    level=log_level,
    format='%(asctime)s  %(levelname)-8s  %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
)
log = logging.getLogger(__name__)

# ──────────────────────────────────────────────────────────────
# 2.  LOAD TIMESTAMP  ← partition column
#
#     Format  : YYYY-MM-DD-HH   (hour-level granularity)
#     Example : 2026-05-16-14
#
#     Using hour granularity gives you:
#       • Fine-grained Athena partition pruning
#       • Easy re-runs: drop one hour's partition and reload
#       • No data skew from mixing multiple hours in one partition
# ──────────────────────────────────────────────────────────────
RUN_TS          = datetime.now(timezone.utc)
LOAD_TIMESTAMP  = RUN_TS.strftime('%Y-%m-%d-%H')   # e.g. 2026-05-16-14
PARTITION_COL   = 'load_timestamp'                  # fixed name — do not change

# ── Resolved paths ────────────────────────────────────────────
SOURCE_PATH = f"s3://{args['source_bucket']}/{args['source_prefix'].strip('/')}/"
TARGET_PATH = f"s3://{args['target_bucket']}/{args['target_prefix'].strip('/')}/"
SOURCE_FMT  = args['source_format'].lower()

log.info("=" * 64)
log.info(f"  JOB NAME      : {args['JOB_NAME']}")
log.info(f"  SOURCE PATH   : {SOURCE_PATH}  [{SOURCE_FMT}]")
log.info(f"  TARGET PATH   : {TARGET_PATH}  [parquet]")
log.info(f"  PARTITION COL : {PARTITION_COL}")
log.info(f"  LOAD TIMESTAMP: {LOAD_TIMESTAMP}")       # ← printed at start
log.info(f"  JOB BOOKMARK  : {args['job_bookmark']}")
log.info("=" * 64)


# ──────────────────────────────────────────────────────────────
# 3.  READ SOURCE DATA
# ──────────────────────────────────────────────────────────────
log.info(f"[STEP 1] Reading source data from: {SOURCE_PATH}")

FORMAT_OPTIONS_MAP = {
    'json':    {'format': 'json',    'format_options': {'multiLine': 'true'}},
    'csv':     {'format': 'csv',     'format_options': {'withHeader': 'true',
                                                        'separator': ',',
                                                        'quoteChar': '"'}},
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
        'groupSize':  '134217728',       # 128 MB per Spark partition
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
# 4.  ENRICHMENT
#     • Sanitise column names
#     • Cast all columns to string  (schema-on-read pattern)
#     • Drop fully-null rows
#     • Stamp audit columns
#     • Attach load_timestamp as the partition column  ← KEY STEP
# ──────────────────────────────────────────────────────────────
log.info("[STEP 2] Enriching data — audit columns + load_timestamp partition")

df = source_dyf.toDF()

# Sanitise column names: lowercase, replace spaces/dashes with underscores
clean_cols = {c: c.strip().lower().replace(' ', '_').replace('-', '_')
              for c in df.columns}
for old, new in clean_cols.items():
    if old != new:
        df = df.withColumnRenamed(old, new)

# Cast every source column to string — no type failures at landing stage
for col_name in df.columns:
    df = df.withColumn(col_name, df[col_name].cast(StringType()))

# Drop rows where every field is null
df = df.dropna(how='all')

# ── Audit + partition columns ─────────────────────────────────
#
#   load_timestamp        → partition column (YYYY-MM-DD-HH)
#                           stamped ONCE per job run so every
#                           record in this batch shares the same
#                           partition key — fast, consistent pruning
#
#   _load_timestamp_full  → full ISO-8601 datetime for lineage
#   _source_path          → exact S3 origin path
#   _source_format        → input format (json/csv/parquet)
#   _job_name             → Glue job name for traceability
#
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

# ── Show schema in debug mode ─────────────────────────────────
if log_level == logging.DEBUG:
    df.printSchema()
    df.show(5, truncate=True)


# ──────────────────────────────────────────────────────────────
# 5.  REPARTITION
#     ~128 MB Parquet files; 2 KB avg row → 65,536 rows per file
# ──────────────────────────────────────────────────────────────
ROWS_PER_PARTITION = 65_536
num_partitions = max(1, enriched_count // ROWS_PER_PARTITION)
log.info(f"[STEP 3] Repartitioning to {num_partitions} output file(s)")
df = df.repartition(num_partitions)


# ──────────────────────────────────────────────────────────────
# 6.  WRITE TO LANDING BUCKET
#
#     Output layout on S3:
#
#       s3://<target_bucket>/<target_prefix>/
#           load_timestamp=2026-05-16-14/
#               part-00000.snappy.parquet
#               part-00001.snappy.parquet
#
#     Each job run writes to its own hour-level partition folder.
#     Re-running the same hour overwrites only that partition.
# ──────────────────────────────────────────────────────────────
log.info(f"[STEP 4] Writing to   : {TARGET_PATH}")
log.info(f"         Partition key : {PARTITION_COL}={LOAD_TIMESTAMP}")

landing_dyf = DynamicFrame.fromDF(df, glueContext, 'landing_dyf')

sink = glueContext.getSink(
    connection_type='s3',
    path=TARGET_PATH,
    enableUpdateCatalog=True,
    updateBehavior='UPDATE_IN_DATABASE',
    partitionKeys=[PARTITION_COL],              # ← load_timestamp drives partitioning
    transformation_ctx='sink',
)
sink.setFormat('glueparquet', format_options={'compression': 'snappy'})
sink.setCatalogInfo(
    catalogDatabase='marketo_landing',
    catalogTableName='leads_raw',
)
sink.writeFrame(landing_dyf)

log.info(f"[STEP 4] Write complete.")


# ──────────────────────────────────────────────────────────────
# 7.  SUMMARY
# ──────────────────────────────────────────────────────────────
log.info("=" * 64)
log.info("  INJECT SUMMARY")
log.info(f"  Source records read    : {record_count:>10,}")
log.info(f"  Records written        : {enriched_count:>10,}")
log.info(f"  Partition column       : {PARTITION_COL}")
log.info(f"  Partition value        : {LOAD_TIMESTAMP}")
log.info(f"  Full run timestamp     : {RUN_TS.strftime('%Y-%m-%dT%H:%M:%SZ')}")
log.info(f"  Target path            : {TARGET_PATH}{PARTITION_COL}={LOAD_TIMESTAMP}/")
log.info(f"  Output format          : parquet / snappy")
log.info("=" * 64)

job.commit()
log.info("[DONE] Job committed successfully.")