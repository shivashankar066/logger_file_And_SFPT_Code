import gspread
from oauth2client.service_account import ServiceAccountCredentials
import pandas as pd

# Define the scope and authenticate
scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
creds = ServiceAccountCredentials.from_json_keyfile_name('C:/Users/srampur/Desktop/my-project-431703-3d4551fc2031.json', scope)
client = gspread.authorize(creds)

# Open the existing Google Sheet by its ID
spreadsheet_id = '1kdiEE4EAGJfCinBl4ycPwqpPGatZhF7g3g11GnOFiNE'  # Replace with your actual spreadsheet ID
spreadsheet = client.open_by_key(spreadsheet_id)
# Print the URL of the created spreadsheet
print("Spreadsheet URL:", spreadsheet.url)
# Open the first worksheet
worksheet = spreadsheet.get_worksheet(0)

# Prepare some data to append
data = {
    'Name': ['Shivashankar', 'Mallesh', 'Sai'],
    'Age': [100, 102, 105]
}
df = pd.DataFrame(data)

# Append the DataFrame to the Google Sheet
worksheet.append_rows(df.values.tolist(), value_input_option='RAW')
spreadsheet.share('mmaguluri@tasl.aero', perm_type='user', role='writer')
print("Data written/Updated to Google Sheets successfully.")
