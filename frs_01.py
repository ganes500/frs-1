import pandas as pd
from typing import Optional, List, Tuple, Dict, Union

import os
import glob
model_collateral_raw=pd.read_csv(r'E:\D\ganesh data\f\DA + Data Science\power bi\major project\data\data\model_collateral.csv')
print(model_collateral_raw)
model_config_raw=pd.read_csv(r'E:\D\ganesh data\f\DA + Data Science\power bi\major project\data\data\model_config.csv')
print(model_config_raw)
# Define folder path
folder_path = "E:/D/ganesh data/f/DA + Data Science/power bi/major project/data/data/model_auth_Rep"

# Get all CSV files in the folder
csv_files = glob.glob(os.path.join(folder_path, "*.csv"))

# Read and combine all CSV files into one DataFrame
df_list = [pd.read_csv(file) for file in csv_files]  # Read each file
model_auth_Rep_raw = pd.concat(df_list, ignore_index=True)  # Combine all data

# Display the first few rows
print(model_auth_Rep_raw)

def validate_dataframe(df: pd.DataFrame,
                       n_cols: Optional[int] = None,
                       n_rows: Optional[Tuple[int, int]] = None,
                       columns: Optional[List[str]] = None,
                       column_types: Optional[Dict[str, type]] = None,
                       check_duplicates: bool = False,
                       check_null_values: bool = False,
                       unique_columns: Optional[List[str]] = None,
                       column_ranges: Optional[Dict[str, Tuple[Union[int, float],
                       Union[int, float]]]] = None,
                       date_columns: Optional[List[str]] = None,
                       categorical_columns: Optional[Dict[str, List[Union[str, int, float]]]] = None
                                ) -> Tuple[bool, str]:
    # Validate number of columns
    if n_cols is not None and len(df.columns) != n_cols:
        return False, f"Error: Expected {n_cols} columns, but found {len(df.columns)} columns."
        # Validate row range
    if n_rows is not None:
        min_rows, max_rows = n_rows
        if not (min_rows <= len(df) <= max_rows):
            return False, f"Error: Number of rows should be between {min_rows} and {max_rows}."

        # Validate columns
    if columns is not None and not set(columns).issubset(df.columns):
        missing_columns = set(columns) - set(df.columns)
        return False, f"Error: Missing columns: {missing_columns}."

        # Validate column types
    if column_types is not None:
        for col, expected_type in column_types.items():
            if col not in df.columns:
                return False, f"Error: Column '{col}' not found."
            if not df[col].dtype == expected_type:
                return False, f"Error: Column '{col}' should have type {expected_type}."

        # Validate duplicates in specific columns
    if check_duplicates and df.duplicated().any():
        return False, "Duplicates found in the DataFrame."

        # Validate null values in specific columns
    if check_null_values and df.isnull().any().any():
        return False, "DataFrame contains null values."

        # Validate unique values in specific columns
    if unique_columns is not None:
        for col in unique_columns:
            if col in df.columns and df[col].duplicated().any():
                return False, f"Column '{col}' should have only unique values."

        # Validate values in a specific range
    if column_ranges is not None:
        for col, value_range in column_ranges.items():
            if col in df.columns and not df[col].between(*value_range).all():
                return False, f"Values in '{col}' should be between {value_range[0]} and {value_range[1]}."

        # Validate date format (assuming 'date_columns' are date columns)
    if date_columns is not None:
        for col in date_columns:
            if col in df.columns:
                try:
                    pd.to_datetime(df[col], errors='raise')
                except ValueError:
                    return False, f"'{col}' should be in a valid date format."

        # Validate categorical values
    if categorical_columns is not None:
        for col, allowed_values in categorical_columns.items():
            if col in df.columns and not df[col].isin(allowed_values).all():
                return False, f"Values in '{col}' should be {allowed_values}."

        # If all validations pass, return True
    return True, "DataFrame has passed all validations."
# Usage example:

model_config = pd.read_csv(r"E:\D\ganesh data\f\DA + Data Science\power bi\major project\data\data\model_config.csv")
is_valid, message = validate_dataframe(model_config, n_cols=4, check_duplicates=True)
print(is_valid, message)

#model_config


# Usage example:
model_collateral = pd.read_csv(r"E:\D\ganesh data\f\DA + Data Science\power bi\major project\data\data\model_collateral.csv")
is_valid, message = validate_dataframe(model_collateral, n_cols=78, check_duplicates=True)

print(is_valid, message)


#reading all files in one dataframes
folder_path ="E:/D/ganesh data/f/DA + Data Science/power bi/major project/data/data/model_auth_Rep"
csv_files = [f for f in os.listdir(folder_path) if f.endswith('.csv')]
dfs = []
# Read CSV files and append dataframes to the list
for csv_file in csv_files:
    model_authorrep = pd.read_csv(os.path.join(folder_path, csv_file))
    dfs.append(model_authorrep)

# Validate each dataframe in the list
for df in dfs:
    is_valid, message = validate_dataframe(df, n_cols=14, check_duplicates=True)
    print(is_valid, message)