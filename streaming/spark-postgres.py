import yaml
import logging
from pyspark.sql import SparkSession # type: ignore
from pyspark.sql.functions import from_json, col # type: ignore
from pyspark.sql.types import StructType, StructField, StringType, FloatType, IntegerType # type: ignore

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

with open('../secrets.yml', 'r') as f:
    secrets = yaml.safe_load(f)

with open('../config.yml', 'r') as f:
    config = yaml.safe_load(f)

def create_spark_session():
    """
    Creates the Spark Session with suitable configs.
    """
    logger.info('Creating Spark session...')
    try:
        # Spark session is established with Kafka jars.
        spark = SparkSession.builder \
            .appName("Spark-Kafka-Integration") \
            .master("local[*]") \
            .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.0,org.postgresql:postgresql:42.2.5") \
            .config("spark.executor.memory", "4g") \
            .config("spark.executor.cores", "2") \
            .getOrCreate()
        spark.sparkContext.setLogLevel("ERROR")
        logger.info('Spark session created successfully! ')
    except Exception as e:
        logger.error(f"Couldn't create the Spark session: {e}")

    return spark

def create_dataframe(spark_session):
    """
    Configures PySpark as a consumer and reads the streaming data to create the initial dataframe.
    """
    try:
        df = spark_session \
            .readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", secrets['upstash_info']['bootstrap_server']) \
            .option("subscribe", config["cluster_variables"]["topic"]) \
            .option("kafka.sasl.mechanism", "SCRAM-SHA-256") \
            .option("kafka.security.protocol", "SASL_SSL") \
            .option("kafka.sasl.jaas.config", 
                    f"org.apache.kafka.common.security.scram.ScramLoginModule required " +
                    f"username='{secrets['upstash_info']['sasl_plain_username']}' " +
                    f"password='{secrets['upstash_info']['sasl_plain_password']}';") \
            .load()
        logger.info("Initial dataframe created successfully")
    except Exception as e:
        logger.warning(f"Initial dataframe couldn't be created due to exception: {e}")

    df = df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
    df = df.withColumn("value", df["value"].cast("string"))
    return df

def create_final_dataframe(df):
    """
    Modifies the initial dataframe and creates the final dataframe.
    """
    schema = StructType([
                StructField("product_id", IntegerType(), False),
                StructField("quantity", IntegerType(), False),
                StructField("price_per_unit", FloatType(), False),
                StructField("timestamp", StringType(), False)
            ])
    df = df.select(from_json(col("value"), schema).alias("data")).select("data.*")

    return df

def start_streaming(df):
    """
    Starts the streaming process to write the DataFrame to PostgreSQL.
    """
    logger.info("Streaming is being started...")
    try:
        df \
            .writeStream \
            .foreachBatch(write_to_postgres) \
            .start() \
            .awaitTermination()
    except Exception as e:
        logger.error(f"Streaming failed: {e}")
        raise

def write_to_postgres(batch_df, batch_id):
    """
    Writes a batch of DataFrame to PostgreSQL.
    """
    try:
        batch_df \
            .write \
            .format("jdbc") \
            .option("url", "jdbc:postgresql://127.0.0.1:5432/airflow") \
            .option("driver", "org.postgresql.Driver") \
            .option("dbtable", config["postgres_variables"]["table_name"]) \
            .option("user", config["postgres_variables"]["user"]) \
            .option("password", config["postgres_variables"]["password"]) \
            .mode("append") \
            .save()
        logger.info(f"Batch {batch_id} written to PostgreSQL successfully")
    except Exception as e:
        logger.error(f"Failed to write batch {batch_id} to PostgreSQL: {e}")
        raise


spark = create_spark_session()
df = create_dataframe(spark)
df = create_final_dataframe(df)

start_streaming(df)

