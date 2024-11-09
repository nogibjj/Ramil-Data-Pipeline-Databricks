import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, concat_ws
import requests
from pyspark.sql.types import (
    StructType, 
    StructField, 
    IntegerType, 
    StringType,
    FloatType
)


def initialize_spark_session(application_name):
    """Initialize and return a Spark session with a specified application name."""
    spark_session = SparkSession.builder.appName(application_name).getOrCreate()
    return spark_session


def terminate_spark_session(spark_session):
    """Stop the Spark session."""
    spark_session.stop()
    return True


def extract_csv(url, file_name, target_directory="data"):
    """Download a CSV file from a URL and save it to a specified directory."""
    if not os.path.exists(target_directory):
        os.makedirs(target_directory)
    with requests.get(url) as response:
        with open(os.path.join(target_directory, file_name), "wb") as file:
            file.write(response.content)
    return os.path.join(target_directory, file_name)


def load_dataframe(spark_session, file_path="data/Clubs.csv"):
    """Load a CSV file into a Spark DataFrame with a predefined schema."""
    olympics_schema = StructType([
        StructField("club_id", IntegerType(), True),
        StructField("club_code", StringType(), True),
        StructField("name", StringType(), True),
        StructField("domestic_competition_id", StringType(), True),
        StructField("total_market_value", StringType(), True), 
        StructField("squad_size", IntegerType(), True),
        StructField("average_age", FloatType(), True),
        StructField("foreigners_number", IntegerType(), True),
        StructField("foreigners_percentage", FloatType(), True),
        StructField("national_team_players", IntegerType(), True),
        StructField("stadium_name", StringType(), True),
        StructField("stadium_seats", IntegerType(), True),
        StructField("net_transfer_record", StringType(), True),  
        StructField("coach_name", StringType(), True),
        StructField("last_season", IntegerType(), True),
        StructField("filename", StringType(), True),
        StructField("url", StringType(), True)
    ])
    
    dataframe = spark_session.read.option("header", "true")\
        .schema(olympics_schema).csv(file_path)
    return dataframe


def run_spark_sql_query(spark_session, dataframe, sql_query, temp_view_name='Clubs'):
    """Run a SQL query on a DataFrame using Spark SQL."""
    dataframe.createOrReplaceTempView(temp_view_name)
    return spark_session.sql(sql_query).show()


def display_summary_statistics(dataframe):
    """Display summary statistics for a DataFrame."""
    dataframe.describe().toPandas().to_markdown()
    return dataframe.describe().show()


def transform_data(df):
    """Apply transformations to add a medal scoring column
      and a combined discipline-event column."""
    transformed_df = df.withColumn(
        "total_market_value_numeric",
        when(col("total_market_value").startswith("€"), 
            col("total_market_value").substr(2, 100).cast("float"))
        .otherwise(None)
    )

    transformed_df = transformed_df.withColumn(
        "market_value_per_player",
        when(
            (col("squad_size").isNotNull()) \
                & (col("total_market_value_numeric").isNotNull()),
            col("total_market_value_numeric") / col("squad_size")
        ).otherwise(None)
    )

    # Transformation: Merge columns into club_summary
    transformed_df = transformed_df.withColumn(
        "club_summary",
        concat_ws(" - ", col("club_code"), col("name"), col("domestic_competition_id"))
    )
    return transformed_df.show()



if __name__ == '__main__':
    extract_csv(
    'https://raw.githubusercontent.com/nogibjj/Ramil-Complex-SQL-Query-MySQL-Database/refs/heads/main/data/clubs.csv',
        file_name = 'Clubs.csv',
        target_directory='data'
    )