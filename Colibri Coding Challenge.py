# Databricks notebook source
# DBTITLE 1,Imports and SparkSession creation
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, mean, stddev, when, min, max, date_trunc
from pyspark.sql.types import StructType, StructField, IntegerType, FloatType, StringType, TimestampType, DoubleType
from pyspark.sql.window import Window
from delta.tables import DeltaTable
from datetime import datetime
import glob

# Initialize Spark session
spark = SparkSession.builder \
    .appName("WindTurbineDataProcessing") \
    .getOrCreate()

# COMMAND ----------

# DBTITLE 1,Set file_paths and schema
# Define file paths assumming we have the data in databricks, if not we can provide the ADLS2 path (after we do the mounting or provide the link to the ADLS2)
source_path = "/FileStore/tables/"
file_paths = [
    f"{source_path}/*data_group_1*.csv", 
    f"{source_path}/*data_group_2*.csv", 
    f"{source_path}/*data_group_3*.csv"
]

# Define a consistent schema
TURBINE_DATA_SCHEMA = StructType([
    StructField("timestamp", TimestampType(), True), 
    StructField("turbine_id", IntegerType(), True),
    StructField("wind_speed", DoubleType(), True),
    StructField("wind_direction", IntegerType(), True),
    StructField("power_output", DoubleType(), True)
])

# Base path for archiving
archive_directory = "/FileStore/tables/Archive"

# COMMAND ----------

# DBTITLE 1,Load data
# Load and combine data
combined_df = spark.read.csv(file_paths, header=True, schema=TURBINE_DATA_SCHEMA)

# COMMAND ----------

# DBTITLE 1,Helper functions
def clean_data(df):
    """
    Cleans the input DataFrame by handling missing values and removing outliers.

    Args:
        df (DataFrame): The input DataFrame to be cleaned.

    Returns:
        DataFrame: The cleaned DataFrame with missing values imputed and outliers removed.
    """
    # Define a global window for mean computation
    window_spec = Window.rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

    # Handle missing values by imputing with column mean
    for col_name in df.columns:
        if col_name not in ["timestamp", "turbine_id"] and df.schema[col_name].dataType == FloatType():
            df = df.withColumn(
                col_name, 
                when(col(col_name).isNull(), F.mean(col_name).over(window_spec)).otherwise(col(col_name))
            )
    
    # Remove outliers using IQR method
    for col_name in df.columns:
        if col_name not in ["timestamp", "turbine_id"] and df.schema[col_name].dataType == FloatType():
            Q1 = df.approxQuantile(col_name, [0.25], 0)[0]
            Q3 = df.approxQuantile(col_name, [0.75], 0)[0]
            IQR = Q3 - Q1
            lower_bound = Q1 - 1.5 * IQR
            upper_bound = Q3 + 1.5 * IQR
            df = df.filter((col(col_name) >= lower_bound) & (col(col_name) <= upper_bound))
    
    return df

def calculate_statistics(df):
    """
    Calculates summary statistics (minimum, maximum, and mean) for power output 
    of each turbine based on its turbine ID.
    Args:
        df (DataFrame): A Spark DataFrame containing turbine data with at least 
                        the columns 'turbine_id' and 'power_output'.

    Returns:
        DataFrame: A new DataFrame containing the following columns:
            - turbine_id: The ID of the turbine.
            - min_power_output: The minimum power output recorded for the turbine.
            - max_power_output: The maximum power output recorded for the turbine.
            - mean_power_output: The average power output recorded for the turbine.
    """
    stats_df = df.groupBy("turbine_id").agg(
        min("power_output").alias("min_power_output"),
        max("power_output").alias("max_power_output"),
        mean("power_output").alias("mean_power_output")
    )
    return stats_df

# Not sure if needed but I think daily statistics of Turbines might be usefull data in the future
def calculate_daily_statistics(df):
    """
    Calculates daily summary statistics (min, max, mean) for all turbines.

    Args:
        df (DataFrame): A Spark DataFrame containing turbine data with at least 
                        the columns 'timestamp', 'turbine_id', and 'power_output'.

    Returns:
        DataFrame: A new DataFrame containing the following columns:
            - date: The date of the statistics.
            - turbine_id: The ID of the turbine.
            - min_power_output: The minimum power output recorded for the turbine on that day.
            - max_power_output: The maximum power output recorded for the turbine on that day.
            - mean_power_output: The average power output recorded for the turbine on that day.
    """
    daily_stats_df = df.groupBy(
        date_trunc("day", "timestamp").alias("date"),
        "turbine_id"
    ).agg(
        min("power_output").alias("min_power_output"),
        max("power_output").alias("max_power_output"),
        mean("power_output").alias("mean_power_output")
    )
    
    return daily_stats_df

def detect_anomalies(df, stats):
    """
    Identifies anomalies in turbine power output based on two standard deviations 
    from the mean for each turbine.

    This function joins the input data with precomputed statistics (mean and standard 
    deviation) for each turbine and flags rows where the power output lies outside 
    the range of ±2 standard deviations from the mean.

    Args:
        df (DataFrame): A Spark DataFrame containing turbine data with columns 
                        'turbine_id' and 'power_output'.
        stats (DataFrame): A Spark DataFrame containing precomputed statistics 
                           for each turbine, including 'turbine_id', 'mean_power_output', 
                           and optionally other summary statistics.

    Returns:
        DataFrame: A new DataFrame with an additional 'anomaly' column:
            - True: Indicates that the row is an anomaly.
            - False: Indicates that the row is within the normal range.
    """
    # Join statistics to the original DataFrame
    joined_df = df.join(stats, on="turbine_id", how="left")

    # Define anomaly detection logic
    anomaly_df = joined_df.withColumn(
        "anomaly",
        ~((col("power_output") >= col("mean_power_output") - 2 * stddev("power_output").over(Window.partitionBy("turbine_id"))) &
          (col("power_output") <= col("mean_power_output") + 2 * stddev("power_output").over(Window.partitionBy("turbine_id"))))
    )
    return anomaly_df

def merge_turbine_data(new_data: DataFrame, delta_table_path: str):
    """
    Merges new turbine data into an existing Delta table, creating the table if it doesn't exist.

    This function performs an upsert operation, updating existing records or inserting new ones
    based on the timestamp and turbine_id. The data is partitioned by timestamp for optimized
    query performance.

    Args:
        new_data (DataFrame): A DataFrame containing the new turbine data to be merged.
        delta_table_path (str): The path where the Delta table is or will be stored.

    Returns:
        None
    """
    if DeltaTable.isDeltaTable(spark, delta_table_path):
        # Load existing Delta table
        delta_table = DeltaTable.forPath(spark, delta_table_path)
        
        # Perform merge operation
        delta_table.alias("existing").merge(
            new_data.alias("new"),
            "existing.timestamp = new.timestamp AND existing.turbine_id = new.turbine_id"
        ).whenMatchedUpdateAll() \
         .whenNotMatchedInsertAll() \
         .execute()
    else:
        # Create a new Delta table if it doesn't exist
        new_data.write.format("delta").partitionBy("timestamp").save(delta_table_path)

    print("Merge operation completed successfully.")

def archive_source_files(source_pattern: str, archive_base_path: str):
    """
    Moves source CSV files matching a pattern to an archive directory structured by date.

    Args:
        source_pattern (str): Glob pattern for the source CSV files (e.g., "/path/to/*.csv").
        archive_base_path (str): Base path for the archive directory.

    Returns:
        None
    """
    # Get the current date in ddmmyy format
    current_date = datetime.now().strftime("%d-%m-%y")
    
    # Create the archive directory for the current date
    archive_date_path = f"{archive_base_path}/{current_date}"
    dbutils.fs.mkdirs(archive_date_path)
    
    # List all files matching the source pattern
    source_files = [file.path for file in dbutils.fs.ls(source_pattern.rsplit('/', 1)[0]) if file.path.endswith(".csv")]
    
    # Move each file to the archive directory
    for source_file in source_files:
        file_name = source_file.split("/")[-1]
        destination_path = f"{archive_date_path}/{file_name}"
        dbutils.fs.mv(source_file, destination_path)
        print(f"Moved {source_file} to {destination_path}")


# COMMAND ----------

# DBTITLE 1,Process
# Process combined dataset if there are more complex logic we might even save the clansed data in a medalion schema(silver layer) to use it later, but for this example we can just use the clansed df and save all of them at the end
cleaned_df = clean_data(combined_df)
statistics_df = calculate_statistics(cleaned_df)
daily_statistics_df = calculate_daily_statistics(cleaned_df)
anomalies_df = detect_anomalies(cleaned_df, statistics_df)

# COMMAND ----------

# DBTITLE 1,Saving results
# Save results as delta table in Databricks DBFS location, but for a better performance TimeScale might be a better option for timeseries data as we have, unfortunately don't have experience with it(yet).
merge_turbine_data(cleaned_df,"/FileStore/tables/CleansedData")
merge_turbine_data(statistics_df,"/FileStore/tables/Statistics")
merge_turbine_data(anomalies_df,"/FileStore/tables/Anomalies")

# COMMAND ----------

# Archive the files since we don't want to ingest the same files again and again (considering the fact that we are running the entire notebook, if it fails at any previous block this won't run either so we can investigate the issues)
archive_source_files(source_path, archive_directory)



