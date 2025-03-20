import pandas as pd
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
