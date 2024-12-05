import pandas as pd
import numpy as np
import os

# Get the directory of the current script
script_dir = os.path.dirname(os.path.abspath(__file__))

# Load the original CSV files
files = ['data_group_1.csv', 'data_group_2.csv', 'data_group_3.csv']
dataframes = [pd.read_csv(os.path.join(script_dir, file)) for file in files]

# Combine all datasets to calculate overall statistics
combined_data = pd.concat(dataframes)
power_mean = combined_data['power_output'].mean()
power_std = combined_data['power_output'].std()

# Define anomaly thresholds
lower_limit = power_mean - 2 * power_std
upper_limit = power_mean + 2 * power_std

# Function to add anomalies to a dataset
def add_anomalies(df, lower, upper):
    """
    Adds anomalies to a DataFrame by modifying power output values.
    This function introduces anomalies to 5% of the data points in the DataFrame,
    using both upper and lower thresholds to define the types of anomalies.

    Args:
        df (pandas.DataFrame): The input DataFrame containing power output data.
        lower (float): The lower bound for determining low anomalies.
        upper (float): The upper bound for determining high anomalies.

    Returns:
        pandas.DataFrame: The DataFrame with added anomalies.
        
    Note:
        - For values below the lower bound, power output is drastically reduced.
        - For values above the upper bound, power output is sharply exaggerated.
        - Anomalies are added to 5% of the data points.
    """
    # Adding anomalies to 5% of the dataset
    anomaly_count = int(len(df) * 0.05)
    anomaly_indices = np.random.choice(df.index, anomaly_count, replace=False)

    # Introducing anomalies based on upper and lower bounds
    for idx in anomaly_indices:
        if df.loc[idx, 'power_output'] < lower:  # Very low anomaly
            df.loc[idx, 'power_output'] *= 0.1  # Reduce drastically
        elif df.loc[idx, 'power_output'] > upper:  # Very high anomaly
            df.loc[idx, 'power_output'] *= 5  # Exaggerate sharply
        # Else values within normal range remain untouched

    return df

# Apply anomalies and save the modified datasets
for i, df in enumerate(dataframes):
    df_with_anomalies = add_anomalies(df.copy(), lower_limit, upper_limit)
    anomaly_file = f'anomalies_data_group_{i + 1}.csv'
    df_with_anomalies.to_csv(anomaly_file, index=False)