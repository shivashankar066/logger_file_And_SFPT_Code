# # Insert all required libraries and credentials
import paramiko
import pandas as pd
import io
import urllib
from sqlalchemy import create_engine

longterm_headers=['Record Type ', 'Company Code ', 'Trading Partner Id',
       'Tracking Number', 'Filler', 'Master Tracking Number', 'Filler.1',
       'Shipping Date', 'Estimated Delivery Date', 'Estimated Delivery Time',
       ' Delivery Date', 'Delivery Time', 'Proof of Delivery Name',
       'Origin Location Code', 'Destination Code', ' Last Status  Date',
       'Status Time', 'GMT-OffSet/Filler', 'Shipper Name',
       'Shipper Company Name', 'Shipper Address1', 'Shipper Address2',
       'Shipper Address3', 'Shipper City Name', 'Shipper State',
       'Shipper Country', 'Shipper Postal Code', 'Shipper Account Reference',
       'Shipper Reference', 'Recipient Name', 'Recipient Company Name',
       'Recipient Address1', 'Recipient Address2', 'Recipient Address3',
       'Recipient City', 'Recipient State', 'Recipient Postal Code',
       'Recipient Country', 'Filler/First Possession', 'Service Code',
       'Package Code', 'Transportation Payor', 'Duty Tax Payor',
       'Track Type Code', 'Filler.2', 'Total Pieces Qty', 'Weight UOM',
       'DIM UOM', 'Filler.3', 'Package Length', 'Package Width',
       'Package Height', 'Purchase Order Nbr', 'Invoice Nbr', 'Department Nbr',
       'Shipment Id', 'LB Package Weight', 'kg Package Weight',
       'Delivery Attempt Exception Cd', 'Last Status Code', 'TCN',
       'Bill of Lading', 'Partner Carrier NBR1', 'Partner Carrier NBR2', 'RMA',
       'Appt Delivery Date', 'Appt Delivery Time', 'Status City',
       'Status State Prov', 'Status Country', 'Cust Delay Reason CD1',
       'Cust Delay Reason CD2', 'Additional Status Info', 'Special Handling 1',
       'Special Handling 2', 'Special Handling 3', 'Special Handling 4',
       'Reconciliation Flag', 'Recipient  Number', 'Status Loc Code',
       'Estimated Delivery Window\n Begin', 'Estimated Delivery Window End',
       'Estimated Delivery Time Offset', 'Filler.4']
# SFTP credentials
host = "sftp.tadl.com"
port = 22
username = "TASL_Fedex@tadl.com"
password = "$E(urefTp@1608)&"

# Connect to SFTP
ssh_client = paramiko.SSHClient()
ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
ssh_client.connect(hostname=host, port=port, username=username, password=password)
print('Connection established successfully')

# Open SFTP session
ftp = ssh_client.open_sftp()

# List all files and directories in the remote directory
remote_directory = "/"
files = ftp.listdir(remote_directory)
# print("Listing all the files and Directory: ", files)

# Exclude specific directories
exclude_dirs = ['Fedex Archive', 'Fedex In', 'test']
csv_files = [file for file in files if file not in exclude_dirs]


# Function to process a single CSV file
def process_file(file_stream, header_mapping):
    df = pd.read_csv(file_stream)

    # Preprocessing: Convert columns like 'TRCK#' and 'MTRK#'
    df['TRCK#'] = df['TRCK#'].fillna('').apply(lambda x: int(float(x)) if x != '' else x)
    df['MTRK#'] = df['MTRK#'].fillna('').apply(lambda x: int(float(x)) if x != '' else x)

    # Map headers
    shortcut_headers = df.columns
    combined_headers = [f"{header_mapping.get(col, col)} ({col})" for col in df.columns]
    df.columns = combined_headers

    return df


# Initialize an empty list to hold all processed DataFrames
all_dfs = []

# Preprocessing CSV files directly from SFTP
for file in csv_files:
    remote_file_path = remote_directory + file
    try:
        # Open the file from the server as a file-like object
        with ftp.file(remote_file_path, 'r') as f:
            file_stream = io.StringIO(f.read().decode('utf-8'))  # Read file from server to a stream

            # Create the mapping for each file based on the shortcut headers
            header_mapping = dict(zip(pd.read_csv(file_stream, nrows=0).columns, longterm_headers))

            # Rewind file stream for reading again
            file_stream.seek(0)

            # Process the file
            df_processed = process_file(file_stream, header_mapping)

            # Append the processed DataFrame to the list
            all_dfs.append(df_processed)

            # Delete the file from the SFTP server after processing
            # ftp.remove(remote_file_path)
            # print(f"Deleted {file} from SFTP server after processing.")
    except Exception as e:
        print(f"Failed to process or delete {file}: {e}")

# Concatenate all DataFrames into one if needed
final_df = pd.concat(all_dfs, ignore_index=True)

# View the head of the final DataFrame
final_df.head()

# Close the SFTP session and SSH connection
ftp.close()
ssh_client.close()


# Define your connection parameters
server = '10.10.9.56'
database = 'AnalyticsDB'
username = 'taslanalytics'
password = 'Secure@2024$%!'

print("Initial data", final_df.head())
df=final_df
print("Initial size of the file", df.shape)

# Mapping with TASL Format
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
 'Origin Location Code (OCODE)': 'Origin Location Code (OCODE)',
 'Destination Code (DCODE)': 'Destination Code (DCODE)',
 ' Last Status  Date (STATD)':'Milestone Date',
 'Status Time (STATC)': 'Milestone Time',
 'GMT-OffSet/Filler (FILL3)': 'GMT-OffSet/Filler (FILL3)',
 'Shipper Name (SHPRN)': 'Shipper Name (SHPRN)',
 'Shipper Company Name (SHPCO)': 'Shipper Name',
 'Shipper Address1 (SHPA1)': 'SHPA1',
 'Shipper Address2 (SHPA2)': 'SHPA2',
 'Shipper Address3 (SHPA3)': 'SHPA3',
 'Shipper City Name (SHPRC)': 'Origin',
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
 'Recipient City (RCPTC)': 'Final Destination',
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
    'OC': 'Booked',
    'PU': 'Picked Up',
    'PX': 'Picked Up',
    'OF': 'Received',
    'CD': 'Customs Clearance',
    'CC': 'Customs Clearance',
    'CP': 'Customs Clearance',
    'DP': 'Departed',
    'FD': 'Arrived',
    'AF': 'Arrived',
    'AR': 'Arrived',
    'SF': 'Arrived',
    'OD': 'Out for Delivery',
    'DL': 'Completed or Delivered',
    'SE': 'Shipment On Hold',
    'DE': 'Shipment On Hold',
    'CA': 'Cancelled',
    'IT': 'Intransit',
    'IX': 'Intransit',
    'LO': 'Intransit',
    'TR': 'Intransit'
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

# # Connection to database
#  Create a connection string
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

columns_to_convert = ['Tracking Number','Invoice Value','TASL PO/Line Item No','Shipping Date (SHIPD)', 'Shipper Postal Code (SHPRZ)', 'TASL Account Code',
                      'Recipient Postal Code (RCPTZ)',' Delivery Date (DELVD)','Delivery Time (DELVT)', 'Proof of Delivery Name (PODNM)',
                      'Recipient  Number (RCPT#)','FILL4','FILL1','FILL2','GMT-OffSet/Filler (FILL3)', 'Service Code (SVCCD)', 'FILL6', 'Shipment Id (SHPID)',
                      'Special Handling 2 (SPHC2)', 'SPHC3', 'SPHC4', 'RECON', 'FILL7', 'Status Loc Code (EVLOC)', 'Estimated Delivery Window\n Begin (EDTWBEG)',
                      'Estimated Delivery Window End (EDTWEND)', 'Estimated Delivery Time Offset (EDTWTZ)','CDRC1','CDRC2','RMA#','PC#1','PC#2',
                      'Bill of Lading (BOL#)','TCN#','Delivery Attempt Exception Cd (DEXCD)','FILL5','SHPA2',
                      'Milestone Current Location','Shipper State (SHPRS)','Status Country (EVECO)','Appt Delivery Date (APPTD)',
                      'Appt Delivery Time (APPTT)','Status State Prov (EVEST)','Invoice No','SHPA3','SIREF','RCPA2','RCPA3']

# Convert specified columns from float to object (string)
df_reordered[columns_to_convert] = df_reordered[columns_to_convert].astype(str)

print(df_reordered.info())

# Define your table name
table_name = "FedEx_Logistic_data_sample1"

# Write the DataFrame to the SQL Server table
try:
    df_reordered.to_sql(table_name, con=engine, index=False, if_exists='append')
    print("Data has been successfully inserted into the SQL Server table.")
except Exception as e:
    print(f"An error occurred: {e}")
