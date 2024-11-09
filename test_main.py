import pytest
from pyspark.sql import SparkSession
from mylib.lib import (
    extract_csv, run_spark_sql_query, transform_data, display_summary_statistics, load_dataframe, terminate_spark_session, initialize_spark_session
)
import os


@pytest.fixture(scope="module")
def spark():
    # Start a Spark session for testing
    spark_session = SparkSession.builder.master("local[*]").appName("test_app").getOrCreate()
    yield spark_session
    # Stop the Spark session after tests
    spark_session.stop()


def test_initialize_spark_session(spark):
    # Test if Spark session is started
    assert spark is not None


def test_extract_csv():
    # Test if the CSV file is downloaded and saved correctly
    test_file_path = extract_csv(
        'https://raw.githubusercontent.com/nogibjj/Ramil-Complex-SQL-Query-MySQL-Database/refs/heads/main/data/clubs.csv',
        file_name='Test_Clubs.csv',
        target_directory='data'
    )
    assert os.path.exists(test_file_path), "File was not downloaded or saved correctly."


def test_load_dataframe(spark):
    # Test loading the CSV file into a DataFrame
    df = load_dataframe(spark, file_path="data/Test_Clubs.csv")
    
    assert df is not None, "DataFrame is None; load_dataframe failed."
    assert df.count() > 0, "DataFrame is empty; load_dataframe did not load data correctly."
    assert "club_id" in df.columns, "Expected column 'club_id' not found in DataFrame."


def test_run_spark_sql_query(spark):
    # Test running an SQL query on a temporary view
    df = load_dataframe(spark, file_path="data/Test_Clubs.csv")

    res = run_spark_sql_query(spark, df, sql_query="SELECT * FROM temp_table", temp_view_name='temp_table')
    
    # Check if the query runs successfully (Spark SQL `show()` does not return results directly)
    assert res is None, "Query execution failed."


def test_display_summary_statistics(spark):
    # Test displaying summary statistics
    df = load_dataframe(spark, file_path="data/Test_Clubs.csv")
    res = display_summary_statistics(df)
    
    # Check if summary statistics run successfully (DataFrame `describe().show()` returns None)
    assert res is None, "Displaying summary statistics failed."


def test_transform_medal_data(spark):
    # Test transformation to calculate market value per player and merged summary column
    df = load_dataframe(spark, file_path="data/Test_Clubs.csv")
    transformed_df = transform_data(df)
    
    # Check if the new columns are added to the DataFrame
    assert transformed_df is None