import os
import pandas as pd

# Set display options for viewing full data
pd.options.display.max_columns = None
pd.options.display.max_rows = None

# Path to the folder containing your CSV files
folder_path = "E:/Logistics Control Tower/TASL_FedEx/New folder_csvfiles/"
header_file = "C:/Users/srampur/Desktop/headers.xlsx"

# Read the header file for mapping
header = pd.read_excel(header_file)
longterm_headers = header.columns

# Function to process a single CSV file
def process_file(file_path, header_mapping):
    df = pd.read_csv(file_path)

    # Preprocessing: Convert columns like 'TRCK#' and 'MTRK#'
    df['TRCK#'] = df['TRCK#'].fillna('').apply(lambda x: int(float(x)) if x != '' else x)
    df['MTRK#'] = df['MTRK#'].fillna('').apply(lambda x: int(float(x)) if x != '' else x)

    # Map headers
    shortcut_headers = df.columns
    combined_headers = [f"{header_mapping.get(col, col)} ({col})" for col in df.columns]
    df.columns = combined_headers

    return df


# Prepare a list of all CSV files in the folder
csv_files = [f for f in os.listdir(folder_path) if f.endswith('.csv')]

# Initialize an empty list to hold all processed DataFrames
all_dfs = []

# Iterate over each CSV file in the folder
for file in csv_files:
    file_path = os.path.join(folder_path, file)

    # Create the mapping for each file based on the shortcut headers
    header_mapping = dict(zip(pd.read_csv(file_path, nrows=0).columns, longterm_headers))

    # Process the file
    df_processed = process_file(file_path, header_mapping)

    # Append the processed DataFrame to the list
    all_dfs.append(df_processed)

# Concatenate all DataFrames into one if needed
final_df = pd.concat(all_dfs, ignore_index=True)
df=final_df
print(df.shape)

new_mapping_dict={'Record Type  (RTYPE)': 'Record Type  (RTYPE)',
 'Company Code  (CCODE)': 'Mode of Shipment',
 'Trading Partner Id (TPIDC)': 'Trading Partner Id (TPIDC)',
 'Tracking Number (TRCK#)': 'Tracking Number',
 'Filler (FILL1)': 'FILL1',
 'Master Tracking Number (MTRK#)': 'MAWB',
 'Filler.1 (FILL2)': 'FILL2',
 'Shipping Date (SHIPD)': 'Shipping Date (SHIPD)',
 'Estimated Delivery Date (ESTDD)': 'Estimated Arrival Date at Final Destination',
 'Estimated Delivery Time (ESTDT)': 'Estimated Arrival Time at Final Destination',
 'Delivery Date (DELVD)': 'Delivery Date (DELVD)',
 'Delivery Time (DELVT)': 'Delivery Time (DELVT)',
 'Proof of Delivery Name (PODNM)': 'Proof of Delivery Name (PODNM)',
 'Origin Location Code (OCODE)': 'Origin',
 'Destination Code (DCODE)': 'Final Destination',
 ' Last Status  Date (STATD)':'Milestone Date',
 'Status Time (STATC)': 'Milestone Time',
 'GMT-OffSet/Filler (FILL3)': 'GMT-OffSet/Filler (FILL3)',
 'Shipper Name (SHPRN)': 'Shipper Name (SHPRN)',
 'Shipper Company Name (SHPCO)': 'Shipper Name',
 'Shipper Address1 (SHPA1)': 'SHPA1',
 'Shipper Address2 (SHPA2)': 'SHPA2',
 'Shipper Address3 (SHPA3)': 'SHPA3',
 'Shipper City Name (SHPRC)': 'Shipper City Name (SHPRC)',
 'Shipper State (SHPRS)': 'Shipper State (SHPRS)',
 'Shipper Country (SHPCC)': 'Shipper Country (SHPCC)',
 'Shipper Postal Code (SHPRZ)': 'Shipper Postal Code (SHPRZ)',
 'Shipper Account Reference (ACCT#)': 'TASL Account Code',
 'Shipper Reference (SIREF)': 'SIREF',
 'Recipient Name (RCPTN)': 'Recipient Name (RCPTN)',
 'Recipient Company Name (RCPCO)': 'Recipient Company Name (RCPCO)',
 'Recipient Address1 (RCPA1)': 'Recipient Address1 (RCPA1)',
 'Recipient Address2 (RCPA2)': 'RCPA2',
 'Recipient Address3 (RCPA3)': 'RCPA3',
 'Recipient City (RCPTC)': 'Recipient City (RCPTC)',
 'Recipient State (RCPTS)': 'Recipient State (RCPTS)',
 'Recipient Postal Code (RCPTZ)': 'Recipient Postal Code (RCPTZ)',
 'Recipient Country (RCPCC)': 'Recipient Country (RCPCC)',
 'Filler/First Possession (FILL4)': 'FILL4',
 'Service Code (SVCCD)': 'Service Code (SVCCD)',
 'Package Code (PKGCD)': 'Package Code (PKGCD)',
 'Transportation Payor (TRPAY)': 'TRPAY',
 'Duty Tax Payor (DTPAY)': 'DTPAY',
 'Track Type Code (TYPCD)': 'TYPCD',
 'Filler.2 (FILL5)': 'FILL5',
 'Total Pieces Qty (PIECS)': 'Total Pieces Qty (PIECS)',
 'Weight UOM (UOMCD)': 'Weight UOM (UOMCD)',
 'DIM UOM (DIMCD)': 'DIMCD',
 'Filler.3 (FILL6)': 'FILL6',
 'Package Length (PKGLN)': 'Package Length (PKGLN)',
 'Package Width (PKGWD)': 'Package Width (PKGWD)',
 'Package Height (PKGHT)': 'Package Height (PKGHT)',
 'Purchase Order Nbr (POREF)': 'TASL PO/Line Item No',
 'Invoice Nbr (INREF)': 'Invoice No',
 'Department Nbr (DEPT#)': 'Invoice Value',
 'Shipment Id (SHPID)': 'Shipment Id (SHPID)',
 'LB Package Weight (LBWGT)': 'LB Package Weight (LBWGT)',
 'kg Package Weight (KGWGT)': 'Weight',
 'Delivery Attempt Exception Cd (DEXCD)': 'Delivery Attempt Exception Cd (DEXCD)',
 'Last Status Code (SCODE)': 'Status',
 'TCN (TCN#)': 'TCN#',
 'Bill of Lading (BOL#)': 'Bill of Lading (BOL#)',
 'Partner Carrier NBR1 (PC#1)': 'PC#1',
 'Partner Carrier NBR2 (PC#2)': 'PC#2',
 'RMA (RMA#)': 'RMA#',
 'Appt Delivery Date (APPTD)': 'Appt Delivery Date (APPTD)',
 'Appt Delivery Time (APPTT)': 'Appt Delivery Time (APPTT)',
 'Status City (ECITY)': 'Milestone Current Location',
 'Status State Prov (EVEST)': 'Status State Prov (EVEST)',
 'Status Country (EVECO)': 'Status Country (EVECO)',
 'Cust Delay Reason CD1 (CDRC1)': 'CDRC1',
 'Cust Delay Reason CD2 (CDRC2)': 'CDRC2',
 'Additional Status Info (AINFO)': 'Additional Status Info (AINFO)',
 'Special Handling 1 (SPHC1)': 'Dangerous Goods',
 'Special Handling 2 (SPHC2)': 'Special Handling 2 (SPHC2)',
 'Special Handling 3 (SPHC3)': 'SPHC3',
 'Special Handling 4 (SPHC4)': 'SPHC4',
 'Reconciliation Flag (RECON)': 'RECON',
 'Recipient  Number (RCPT#)': 'Recipient  Number (RCPT#)',
 'Status Loc Code (EVLOC)': 'Status Loc Code (EVLOC)',
 'Estimated Delivery Window\n Begin (EDTWBEG)': 'Estimated Delivery Window\n Begin (EDTWBEG)',
 'Estimated Delivery Window End (EDTWEND)': 'Estimated Delivery Window End (EDTWEND)',
 'Estimated Delivery Time Offset (EDTWTZ)': 'Estimated Delivery Time Offset (EDTWTZ)',
 'Filler.4 (FILL7)': 'FILL7'}

df_renamed = df.rename(columns=new_mapping_dict)

df_renamed['Estimated Arrival Date at Final Destination'] = pd.to_numeric(df_renamed['Estimated Arrival Date at Final Destination'], errors='coerce')
#df_renamed['Estimated Arrival Date at Final Destination'] = df_renamed['Estimated Arrival Date at Final Destination'].astype(str).str.rstrip('.0')
df_renamed['Estimated Arrival Date at Final Destination'] = pd.to_datetime(df_renamed['Estimated Arrival Date at Final Destination'], format='%Y%m%d', errors='coerce')
df_renamed['Estimated Arrival Date at Final Destination'] = df_renamed['Estimated Arrival Date at Final Destination'].dt.strftime('%Y-%m-%d')

df_renamed['Estimated Arrival Time at Final Destination'] = pd.to_numeric(df_renamed['Estimated Arrival Time at Final Destination'], errors='coerce')
def convert_to_mmss(time):
    if pd.isna(time):
        return 'NaN'
    time_str = f'{int(time):04}'  # Convert to a 4-digit string with leading zeros
    minutes = time_str[:2]
    seconds = time_str[2:]
    return f'{minutes}:{seconds}'

df_renamed['Estimated Arrival Time at Final Destination'] = df_renamed['Estimated Arrival Time at Final Destination'].apply(convert_to_mmss)

df_renamed['Milestone Date'] = pd.to_numeric(df_renamed['Milestone Date'], errors='coerce')
# df_renamed['Estimated Arrival Date at Final Destination'] = df_renamed['Estimated Arrival Date at Final Destination'].astype(str).str.rstrip('.0')
df_renamed['Milestone Date'] = pd.to_datetime(df_renamed['Milestone Date'], format='%Y%m%d', errors='coerce')
df_renamed['Milestone Date'] = df_renamed['Milestone Date'].dt.strftime('%Y-%m-%d')

df_renamed['Milestone Time'] = pd.to_numeric(df_renamed['Milestone Time'], errors='coerce')
def convert_to_mmss(time):
    if pd.isna(time):
        return 'NaN'
    time_str = f'{int(time):04}'  # Convert to a 4-digit string with leading zeros
    minutes = time_str[:2]
    seconds = time_str[2:]
    return f'{minutes}:{seconds}'

df_renamed['Milestone Time'] = df_renamed['Milestone Time'].apply(convert_to_mmss)

# Define a function to handle concatenation while ignoring NaN values
def combine_date_time(date, time):
    if pd.isna(date) or pd.isna(time):
        return None  # Return NaT (Not a Time) for missing values
    return f'{date} {time}'

# Apply the function to create the new column
df_renamed['Estimated Arrival at Final Destination'] = df_renamed.apply(
    lambda row: combine_date_time(row['Estimated Arrival Date at Final Destination'],
                                  row['Estimated Arrival Time at Final Destination']), axis=1
)

# Convert the concatenated column to datetime
df_renamed['Estimated Arrival at Final Destination'] = pd.to_datetime(df_renamed['Estimated Arrival at Final Destination'], errors='coerce')

# Define a function to handle concatenation while ignoring NaN values
def combine_date_time(date, time):
    if pd.isna(date) or pd.isna(time):
        return None  # Return NaT (Not a Time) for missing values
    return f'{date} {time}'

# Apply the function to create the new column
df_renamed['Milestone Datetime'] = df_renamed.apply(
    lambda row: combine_date_time(row['Milestone Date'],
                                  row['Milestone Time']), axis=1
)

# Convert the concatenated column to datetime
df_renamed['Milestone Datetime'] = pd.to_datetime(df_renamed['Milestone Datetime'], errors='coerce')

df_renamed['Package Length (PKGLN)'] = pd.to_numeric(df_renamed['Package Length (PKGLN)'], errors='coerce')
df_renamed['Package Width (PKGWD)'] = pd.to_numeric(df_renamed['Package Width (PKGWD)'], errors='coerce')
df_renamed['Package Height (PKGHT)'] = pd.to_numeric(df_renamed['Package Height (PKGHT)'], errors='coerce')
df_renamed['Volume'] = df_renamed['Package Length (PKGLN)'] * df_renamed['Package Width (PKGWD)'] * df_renamed['Package Height (PKGHT)']

df_renamed['Logistic Partner'] = 'FedEx'
df_renamed['Mode of Transport'] = 'AIR'
df_renamed['To Location'] = ''
df_renamed['Arrival date'] = ''
df_renamed['Ship Carrier/Flight Name']=''
df_renamed['Ship Carrier No/Flight No']=''
df_renamed['Invoice Date']=''
df_renamed['TASL Account Name']=''
df_renamed['Import/Export']=''

# List of columns to appear first
columns_first = ['Logistic Partner','Mode of Transport','Tracking Number', 'MAWB', 'Volume', 'Weight','Status', 'Milestone Current Location',
                 'To Location','Milestone Datetime','Arrival date','Estimated Arrival at Final Destination', 'Origin','Final Destination',
                 'TASL PO/Line Item No', 'Dangerous Goods', 'Ship Carrier/Flight Name','Ship Carrier No/Flight No','Invoice No', 'Invoice Value',
                 'Invoice Date','Shipper Name','TASL Account Code',	'TASL Account Name', 'Mode of Shipment','Import/Export']
# Get the remaining columns by excluding the ones in columns_first
remaining_columns = [col for col in df_renamed.columns if col not in columns_first]
df_reordered = df_renamed[columns_first + remaining_columns]

# Define the mapping dictionary
status_mapping = {
    'OC': 'Booked/Request',
    'PU': 'Picked Up',
    'PX': 'Picked Up',
    'IT': 'Received',
    'IX': 'Received',
    'LO': 'Received',
    'OF': 'Received',
    'CD': 'Customs Clearance',
    'CC': 'Customs Clearance',
    'CP': 'Customs Clearance',
    'TR': 'Departed',
    'DP': 'Departed',
    'FD': 'Arrived',
    'AF': 'Arrived',
    'AR': 'Arrived',
    'SF': 'Arrived',
    'OD': 'Gated Out Terminal',
    'DL': 'Completed or Delivered',
    'SE': 'Shipment On Hold',
    'DE': 'Shipment On Hold',
}

# Replace the Status codes with their corresponding descriptions
df_reordered['Status'] = df_reordered['Status'].map(status_mapping)

# Mode of Shipment=‘E’=Express, 'F'=Freight, 'G'=Ground, 'S'=SmartPost
mode_of_shipment={
    'E': 'Express',
    'F': 'Freight',
    'G': 'Ground',
    'S': 'SmartPost'
}

df_reordered['Mode of Shipment'] = df_reordered['Mode of Shipment'].map(mode_of_shipment)

mapping = {
    674118678: 'Import',
    674118236: 'Import',
    674079354: 'Import',
    337120873: 'Import',
    674117779: 'Import',
    795053310: 'Export',
    202035436: 'Import',
    202035437: 'Export',
    396901366: 'Import'
}

# Use the map function to populate the 'Import/Export' column based on 'TASL Account code'
df_reordered['Import/Export'] = df_reordered['TASL Account Code'].map(mapping)

account_mapping = {
    674118678: 'TLMAL (H03)',
    674118236: 'TSAL (H02)',
    674079354: 'TBAL (H05)',
    337120873: 'TCOE (H06)',
    674117779: 'TASL Hyd (H01)',
    795053310: 'TASL Hyd (H01)',
    202035436: 'TASL-C295 MCA (H07)',
    202035437: 'TASL-C295 MCA (H07)',
    396901366: 'TASL-BLR (B01)'
}

# Use the map function to populate the 'Import/Export' column based on 'TASL Account code'
df_reordered['TASL Account Name'] = df_reordered['TASL Account Code'].map(account_mapping)
df_reordered['Dangerous Goods']=df_reordered['Dangerous Goods'].replace({6:'Yes'}).apply(lambda x: 'No' if x != 'Yes' else x)

print(df_reordered.head())
print(df_reordered.shape)


# Insert all records into database
import pandas as pd
import urllib
from sqlalchemy import create_engine

# Define your connection parameters
server = '10.10.9.56'
database = 'AnalyticsDB'
username = 'taslanalytics'
password = 'Secure@2024$%!'

# Create a connection string
connection_string = (
    f"DRIVER={{ODBC Driver 17 for SQL Server}};"
    f"SERVER={server};"
    f"DATABASE={database};"
    f"UID={username};"
    f"PWD={password}"
)

# Encode the connection string for SQLAlchemy
connection_url = f"mssql+pyodbc:///?odbc_connect={urllib.parse.quote_plus(connection_string)}"

# Create a SQLAlchemy engine
engine = create_engine(connection_url)
print("Database Connection is Successful")

df_reordered['Tracking Number'] = df_reordered['Tracking Number'].astype(str)
df_reordered['Invoice Value'] = df_reordered['Invoice Value'].astype(str)
df_reordered['TASL PO/Line Item No'] = df_reordered['TASL PO/Line Item No'].astype(str)
df_reordered['Shipping Date (SHIPD)'] = df_reordered['Shipping Date (SHIPD)'].astype(str)
df_reordered['Shipper Postal Code (SHPRZ)']= df_reordered['Shipper Postal Code (SHPRZ)'].astype(str)
df_reordered['TASL Account Code']= df_reordered['TASL Account Code'].astype(str)
df_reordered['Recipient Postal Code (RCPTZ)']= df_reordered['Recipient Postal Code (RCPTZ)'].astype(str)
df_reordered[' Delivery Date (DELVD)']= df_reordered[' Delivery Date (DELVD)'].astype(str)
df_reordered['Delivery Time (DELVT)']= df_reordered['Delivery Time (DELVT)'].astype(str)
df_reordered['Proof of Delivery Name (PODNM)']= df_reordered['Proof of Delivery Name (PODNM)'].astype(str)
df_reordered['Recipient  Number (RCPT#)']= df_reordered['Recipient  Number (RCPT#)'].astype(str)
df_reordered['FILL4']= df_reordered['FILL4'].astype(str)
df_reordered['FILL1']= df_reordered['FILL1'].astype(str)
df_reordered['FILL2']= df_reordered['FILL2'].astype(str)
df_reordered['GMT-OffSet/Filler (FILL3)']= df_reordered['GMT-OffSet/Filler (FILL3)'].astype(str)
df_reordered['Service Code (SVCCD)']= df_reordered['Service Code (SVCCD)'].astype(str)
df_reordered['FILL6']= df_reordered['FILL6'].astype(str)
df_reordered['Shipment Id (SHPID)']= df_reordered['Shipment Id (SHPID)'].astype(str)

columns_to_convert = ['Special Handling 2 (SPHC2)', 'SPHC3', 'SPHC4', 'RECON', 'FILL7', 'Status Loc Code (EVLOC)', 'Estimated Delivery Window\n Begin (EDTWBEG)',
                      'Estimated Delivery Window End (EDTWEND)', 'Estimated Delivery Time Offset (EDTWTZ)','CDRC1','CDRC2','RMA#','PC#1','PC#2',
                      'Bill of Lading (BOL#)','TCN#','Delivery Attempt Exception Cd (DEXCD)','FILL5','SHPA2',
                      'Milestone Current Location','Shipper State (SHPRS)','Status Country (EVECO)','Appt Delivery Date (APPTD)',
                      'Appt Delivery Time (APPTT)','Status State Prov (EVEST)','Invoice No','SHPA3','SIREF','RCPA2','RCPA3']

# Convert specified columns from float to object (string)
df_reordered[columns_to_convert] = df_reordered[columns_to_convert].astype(str)

# Define your table name
table_name = "FedEx_Logistic_data1"

# Write the DataFrame to the SQL Server table
try:
    df_reordered.to_sql(table_name, con=engine, index=False, if_exists='append')
    print("Data has been successfully inserted into the SQL Server table.")
except Exception as e:
    print(f"An error occurred: {e}")

