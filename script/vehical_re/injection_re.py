"""
glue_inject_raw_to_landing.py
------------------------------
AWS Glue ETL Script — Stage 1: INJECT
Reads raw data from a source S3 bucket and writes it
to the landing (injection) S3 bucket in Parquet format.

Partition column: load_timestamp  (format: YYYY-MM-DD-HH)

ROOT CAUSE OF ERROR:
  The CSV file has an unquoted JSON column, e.g.:

    transaction_id,vin,...,service_type,[{"vin":"n901398e","driver_id":"drv_261",
    "speed":112,"lat":30.445263,"long":-79.134266,"event_timestamp":"..."}],driver_id,...

  The CSV parser splits on every comma INSIDE the JSON, turning each
  key-value pair into a broken column name like `"lat":_30`.`445263`.

FIX:
  Read the file as raw text lines → reconstruct each row by detecting
  the JSON block (between first `[` and last `]`) → parse JSON separately
  → join back with the regular CSV columns.
"""

import sys
import re
import json
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
#
#     For CSV with embedded unquoted JSON we MUST read as raw
#     text first — standard CSV parsers break on the commas
#     inside the JSON object.
#
#     For JSON / Parquet we use the normal Glue path.
# ──────────────────────────────────────────────────────────────
log.info(f"[STEP 1] Reading source data from: {SOURCE_PATH}")


def read_csv_with_embedded_json(spark, path: str):
    """
    Reads a CSV file that contains an unquoted JSON column.

    Expected CSV structure (columns may vary in count):
      transaction_id, vin, ..., service_type,
      [{"vin":"...", "lat":30.4, "long":-79.1, ...}],
      driver_id, ..., fuel_type

    Strategy:
      1. Read every line as plain text
      2. Split header normally (no JSON in header)
      3. For each data line — isolate the JSON block
         between the FIRST '[' and the LAST ']',
         replace it with a safe placeholder, split on
         commas, then restore the JSON
      4. Build a Row per line and create a DataFrame
    """
    from pyspark.sql import Row

    raw_lines = spark.read.text(path).rdd.map(lambda r: r[0])

    # ── Header ────────────────────────────────────────────────
    header_line = raw_lines.first()
    header      = [h.strip().strip('"') for h in header_line.split(',')]

    # ── Data lines (skip header) ──────────────────────────────
    data_lines = raw_lines.zipWithIndex() \
                          .filter(lambda x: x[1] > 0) \
                          .map(lambda x: x[0])

    PLACEHOLDER = '##JSON_BLOCK##'

    def parse_line(line: str):
        """
        Extract the JSON block (between first [ and last ]),
        replace with placeholder, split on comma, restore JSON.
        Returns a dict matching the header columns.
        """
        try:
            # Find JSON array block boundaries
            json_start = line.find('[')
            json_end   = line.rfind(']')

            if json_start != -1 and json_end != -1 and json_end > json_start:
                json_block   = line[json_start: json_end + 1]
                safe_line    = line[:json_start] + PLACEHOLDER + line[json_end + 1:]
            else:
                json_block = None
                safe_line  = line

            # Split the safe line on commas
            parts = [p.strip().strip('"') for p in safe_line.split(',')]

            # Restore JSON block into its placeholder position
            restored = []
            for p in parts:
                if PLACEHOLDER in p:
                    restored.append(json_block or p)
                else:
                    restored.append(p)

            # Map to header — pad or trim if column count differs
            if len(restored) < len(header):
                restored += [None] * (len(header) - len(restored))
            else:
                restored = restored[:len(header)]

            return Row(**dict(zip(header, restored)))

        except Exception:
            # Return a null row on parse failure — dropped later
            return Row(**{h: None for h in header})

    rows_rdd = data_lines.map(parse_line)
    return spark.createDataFrame(rows_rdd)


# ── Choose read strategy by format ───────────────────────────
if SOURCE_FMT == 'csv':
    log.info("[STEP 1] CSV detected — using raw-text reader to handle embedded JSON")
    df = read_csv_with_embedded_json(spark, SOURCE_PATH).cache()

elif SOURCE_FMT == 'json':
    source_dyf = glueContext.create_dynamic_frame.from_options(
        connection_type='s3',
        connection_options={'paths': [SOURCE_PATH], 'recurse': True},
        format='json',
        format_options={'multiLine': 'true'},
        transformation_ctx='source_dyf',
    )
    df = source_dyf.toDF().cache()

elif SOURCE_FMT == 'parquet':
    source_dyf = glueContext.create_dynamic_frame.from_options(
        connection_type='s3',
        connection_options={'paths': [SOURCE_PATH], 'recurse': True},
        format='parquet',
        format_options={},
        transformation_ctx='source_dyf',
    )
    df = source_dyf.toDF().cache()

else:
    raise ValueError(
        f"Unsupported source_format '{SOURCE_FMT}'. "
        f"Must be one of: json | csv | parquet"
    )

record_count = df.count()
log.info(f"[STEP 1] Records read             : {record_count:,}")
log.info(f"[STEP 1] Columns detected         : {df.columns}")

if record_count == 0:
    log.warning("[STEP 1] No new records found — nothing to inject. Exiting.")
    job.commit()
    sys.exit(0)


# ──────────────────────────────────────────────────────────────
# 4.  PARSE JSON COLUMN  (col1 or any column holding JSON array)
#
#     After raw-text read, the JSON block is preserved as a
#     single string in its column.  Now we explode it into
#     individual typed fields.
#
#     JSON schema inside the array element:
#       { "vin":             string,
#         "driver_id":       string,
#         "speed":           integer,
#         "lat":             double,
#         "long":            double,
#         "event_timestamp": string  }
# ──────────────────────────────────────────────────────────────
EVENT_JSON_SCHEMA = ArrayType(StructType([
    StructField("vin",             StringType(),  True),
    StructField("driver_id",       StringType(),  True),
    StructField("speed",           IntegerType(), True),
    StructField("lat",             DoubleType(),  True),
    StructField("long",            DoubleType(),  True),
    StructField("event_timestamp", StringType(),  True),
]))

# Detect the column that starts with '[' (JSON array column)
json_col = None
for c in df.columns:
    sample = df.select(c).dropna().limit(1).collect()
    if sample and str(sample[0][0]).strip().startswith('['):
        json_col = c
        break

if json_col:
    log.info(f"[STEP 2a] JSON array column detected: '{json_col}' — parsing now")

    df = df.withColumn("_events", F.from_json(F.col(json_col), EVENT_JSON_SCHEMA))

    # Take first element of the array (adjust with explode if multiple events per row)
    df = (df
          .withColumn("event_vin",        F.col("_events")[0]["vin"])
          .withColumn("event_driver_id",  F.col("_events")[0]["driver_id"])
          .withColumn("event_speed",      F.col("_events")[0]["speed"].cast(StringType()))
          .withColumn("event_lat",        F.col("_events")[0]["lat"].cast(StringType()))
          .withColumn("event_long",       F.col("_events")[0]["long"].cast(StringType()))
          .withColumn("event_timestamp",  F.col("_events")[0]["event_timestamp"])
         )

    df = df.drop("_events", json_col)
    log.info("[STEP 2a] Extracted: event_vin, event_driver_id, event_speed, "
             "event_lat, event_long, event_timestamp")
else:
    log.info("[STEP 2a] No JSON array column detected — skipping JSON parse")


# ──────────────────────────────────────────────────────────────
# 5.  ENRICHMENT
# ──────────────────────────────────────────────────────────────
log.info("[STEP 2] Enriching — sanitize columns + audit + partition stamp")

# Sanitise column names
clean_cols = {
    c: c.strip().lower().replace(' ', '_').replace('-', '_')
    for c in df.columns
}
for old, new in clean_cols.items():
    if old != new:
        df = df.withColumnRenamed(old, new)

# Cast everything to string at landing stage
for col_name in df.columns:
    df = df.withColumn(col_name, df[col_name].cast(StringType()))

# Drop fully-null rows
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
log.info(f"[STEP 2] load_timestamp applied       : {LOAD_TIMESTAMP}")
log.info(f"[STEP 2] Final columns                : {df.columns}")

if log_level == logging.DEBUG:
    df.printSchema()
    df.show(5, truncate=False)


# ──────────────────────────────────────────────────────────────
# 6.  REPARTITION
# ──────────────────────────────────────────────────────────────
ROWS_PER_PARTITION = 65_536
num_partitions     = max(1, enriched_count // ROWS_PER_PARTITION)
log.info(f"[STEP 3] Repartitioning to {num_partitions} file(s)")
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