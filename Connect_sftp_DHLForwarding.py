import paramiko
# Create SSH client
ssh_client = paramiko.SSHClient()

# Remote server credentials
host = "sftp3-test.dhl.com"
username = "s8m27qxl"
password = "x2n5NC!z86i2_ynJ"
port = 4222

ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
ssh_client.connect(hostname=host, port=port, username=username, password=password)
print('Connection established successfully')

# Open SFTP session
ftp = ssh_client.open_sftp()

# List all files and directories in the remote directory
remote_directory = "/out/work/"
files = ftp.listdir(remote_directory)
print("Listing all the files and Directory: ", files)

# Download files from remote directory to local directory
for file in files:
    remote_file_path = remote_directory + file
    local_file_path = "E:/Logistic_Project/DHL_Forwarding/new_files/" + file
    ftp.get(remote_file_path, local_file_path)
    print(f"Downloaded {file} to {local_file_path}")

# Close the connection
ftp.close()
ssh_client.close()
