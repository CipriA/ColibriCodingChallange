# Wind Turbine Data Processing Solution

## Overview

This solution is designed to process wind turbine data using **Apache Spark** within a **Databricks** environment. The workflow includes data loading, cleaning, analysis, and archiving steps to effectively manage and derive insights from turbine data.

---

## Key Components

### 1️ **Data Loading**
- Defines file paths for multiple CSV files containing turbine data.
- Establishes a consistent schema for:
  - `timestamp`
  - `turbine_id`
  - `wind_speed`
  - `wind_direction`
  - `power_output`

---

### 2️ **Data Cleaning**
- Implements a `clean_data` function to:
  - Handle missing values by imputing column means.
  - Remove outliers using the **Interquartile Range (IQR)** method.

---

### 3️ **Statistical Analysis**
- The `calculate_statistics` function computes summary statistics:
  - Minimum (`min`)
  - Maximum (`max`)
  - Mean (`mean`) for each turbine's power output.
- The `calculate_daily_statistics` function provides daily statistics for turbines.

---

### 4️ **Anomaly Detection**
- The `detect_anomalies` function identifies anomalies in power output based on:
  - Statistical thresholds (±2 standard deviations from the mean).
- Joins the original data with precomputed statistics to flag anomalies.

---

### 5️ **Data Storage**
- Utilizes **Delta Lake** format for:
  - Efficient storage.
  - ACID transactions.
  - Time travel capabilities.
- The `merge_turbine_data` function performs upsert operations based on:
  - `timestamp`
  - `turbine_id`.

---

### 6️ **Archiving**
- The `archive_source_files` function moves processed CSV files to a date-based archive directory:

Archive/<dd-mm-yy>/<filename>.csv
text
- Ensures no duplicate ingestion of previously processed files.

---

## Assumptions

1. **Data Consistency**: All input CSV files follow the same schema with valid data types.
2. **Sufficient Compute Resources**: Databricks cluster has adequate resources for processing.
3. **Time Zone Consistency**: All timestamps are in a consistent time zone or UTC.
4. **Normal Distribution**: Power output follows a roughly normal distribution for anomaly detection.
5. **File Naming Convention**: Input files are named consistently for easy location.
6. **Error Handling**: Execution halts if any step fails, allowing for investigation.
7. **Archiving Logic**: Files are moved only after successful processing.
8. **Delta Table Management**: Appropriate permissions are set for reading and writing Delta tables.

---

## Usage

1. To use the `Colibri Coding Challenge.py` notebook, you'll have to set up the Databricks environment with the necessary configurations and a standard cluster with Standard_f4 driver node type, no init scripts needed 1-2 workers should be more than enough for the quantity of the data(feel free to add more if the data is larger) and the spark_version should be `11.3.x-scala2.12` or higher.
2. Update file paths and schemas as needed (path used in the notebook for the source csv files should be in DBFS under `/FileStore/tables/`).
3. Run the notebook to process the wind turbine data.
4. Access the processed data in Delta tables for further analysis.
5. Optional if you want to create data with anomalies based on the given source csv files you can use `create_turbine_data.py` to do so. The script loads the original CSV files, calculates overall statistics for power output, defines anomaly thresholds based on 2 standard deviations from the mean, applies the add_anomalies function to each dataset and saves the modified datasets with anomalies as new CSV files.
