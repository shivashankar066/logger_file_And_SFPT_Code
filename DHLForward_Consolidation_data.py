import pandas as pd
import glob

# Specify the path where all CSV files are located
file_path = 'E:/Logistic_Project/DHL_Forwarding/D807130_csv/*.csv'

# Use glob to get all file names matching the pattern
all_files = glob.glob(file_path)
# Create an empty list to hold dataframes
df_list = []

# Loop through all files and append them to the list
for file in all_files:
    df = pd.read_csv(file)
    df_list.append(df)
# Concatenate all dataframes in the list into one
consolidated_df = pd.concat(df_list, ignore_index=True)
# Sort the consolidated dataframe by 'Milestone Datetime' in ascending order
consolidated_df = consolidated_df.sort_values(by='Milestone Datetime')
# Specify the path and name for the consolidated CSV file
output_file = 'E:/Logistic_Project/DHL_Forwarding/D807130_csv/consolidated_file.xlsx'
# Write the consolidated dataframe to a new CSV file
consolidated_df.to_excel(output_file, index=False)
print(f"Consolidated CSV file created successfully at {output_file}.")
