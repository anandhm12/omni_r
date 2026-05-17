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
from pyspark.sql.types import StringType

# ──────────────────────────────────────────────────────────────
# 1. JOB INIT
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
    'AWS_REGION'
])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Logging
log_level = getattr(logging, args.get('log_level', 'INFO').upper(), logging.INFO)
logging.basicConfig(level=log_level)
log = logging.getLogger(__name__)

# ──────────────────────────────────────────────────────────────
# CONFIG
# ──────────────────────────────────────────────────────────────
RUN_TS = datetime.now(timezone.utc)
LOAD_TIMESTAMP = RUN_TS.strftime('%Y-%m-%d-%H')
PARTITION_COL = 'load_timestamp'
CATALOG_DB = 'poc-bootcamp-anand'

# Dynamic table name
safe_prefix = args['source_prefix'].replace('/', '_').replace('-', '_')
CATALOG_TABLE = f"leads_raw_{safe_prefix}"

EXPECTED_COLUMNS = {
    "event_vin",
    "event_driver_id",
    "event_speed",
    "event_lat",
    "event_long",
    "event_timestamp"
}

def clean_bucket(val: str) -> str:
    return val.replace('s3://', '').strip('/')

SOURCE_PATH = f"s3://{clean_bucket(args['source_bucket'])}/{args['source_prefix']}/"
TARGET_PATH = f"s3://{clean_bucket(args['target_bucket'])}/{args['target_prefix']}/"

# ──────────────────────────────────────────────────────────────
# READ DATA
# ──────────────────────────────────────────────────────────────
log.info("Reading source data...")

source_dyf = glueContext.create_dynamic_frame.from_options(
    connection_type='s3',
    connection_options={'paths': [SOURCE_PATH], 'recurse': True},
    format=args['source_format']
)

record_count = source_dyf.count()

if record_count == 0:
    log.warning("No data found. Exiting.")
    job.commit()
    sys.exit(0)

df = source_dyf.toDF()

# ──────────────────────────────────────────────────────────────
# ENRICHMENT
# ──────────────────────────────────────────────────────────────
df = df.dropna(how='all')

for col in df.columns:
    df = df.withColumn(col, df[col].cast(StringType()))

df = df.withColumn(PARTITION_COL, F.lit(LOAD_TIMESTAMP))

# ──────────────────────────────────────────────────────────────
# SCHEMA VALIDATION
# ──────────────────────────────────────────────────────────────
log.info("Validating schema...")

incoming_columns = set(df.columns)

missing_cols = EXPECTED_COLUMNS - incoming_columns
extra_cols = incoming_columns - EXPECTED_COLUMNS

if missing_cols:
    log.warning(f"Missing columns: {missing_cols}")

if extra_cols:
    log.warning(f"Extra columns: {extra_cols}")

# ──────────────────────────────────────────────────────────────
# AWS GLUE CLIENT
# ──────────────────────────────────────────────────────────────
glue_client = boto3.client('glue', region_name=args.get('AWS_REGION', 'us-east-1'))

# ──────────────────────────────────────────────────────────────
# CHECK DATABASE
# ──────────────────────────────────────────────────────────────
try:
    glue_client.get_database(Name=CATALOG_DB)
    log.info(f"Database exists: {CATALOG_DB}")
except ClientError as e:
    if e.response['Error']['Code'] == 'EntityNotFoundException':
        log.info(f"Creating database: {CATALOG_DB}")
        glue_client.create_database(DatabaseInput={'Name': CATALOG_DB})
    else:
        raise e

# ──────────────────────────────────────────────────────────────
# CHECK TABLE
# ──────────────────────────────────────────────────────────────
try:
    glue_client.get_table(DatabaseName=CATALOG_DB, Name=CATALOG_TABLE)
    log.info(f"Table exists: {CATALOG_TABLE}")
except ClientError as e:
    if e.response['Error']['Code'] == 'EntityNotFoundException':
        log.info(f"Table will be created: {CATALOG_TABLE}")
    else:
        raise e

# ──────────────────────────────────────────────────────────────
# WRITE DATA + ATHENA TABLE
# ──────────────────────────────────────────────────────────────
log.info("Writing to S3 and updating Athena...")

landing_dyf = DynamicFrame.fromDF(df, glueContext, "landing_dyf")

sink = glueContext.getSink(
    connection_type='s3',
    path=TARGET_PATH,
    enableUpdateCatalog=True,
    updateBehavior='UPDATE_IN_DATABASE',
    partitionKeys=[PARTITION_COL],
)

sink.setFormat("glueparquet")
sink.setCatalogInfo(
    catalogDatabase=CATALOG_DB,
    catalogTableName=CATALOG_TABLE
)

sink.writeFrame(landing_dyf)

# ──────────────────────────────────────────────────────────────
# COMPLETE
# ──────────────────────────────────────────────────────────────
log.info("Job completed successfully.")
job.commit()