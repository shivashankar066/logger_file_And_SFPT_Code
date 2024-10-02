import paramiko

# Enable paramiko logging
paramiko.util.log_to_file("paramiko.log")

# Create SSH client
ssh_client = paramiko.SSHClient()

# Remote server credentials
host = "sftp.tadl.com"
username = "TASL_Fedex@tadl.com"
password = "$E(urefTp@1608)&"
port = 443  # Changed to default SFTP port for testing

try:
    ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    ssh_client.connect(hostname=host, port=port, username=username, password=password)
    print('Connection established successfully')

    # Open SFTP session
    ftp = ssh_client.open_sftp()

    # List all files and directories in the remote directory
    remote_directory = "/TASL_Fedex/test/"
    files = ftp.listdir(remote_directory)
    print("Listing all the files and Directory: ", files)

    # Download files from remote directory to local directory
    for file in files:
        remote_file_path = remote_directory + file
        local_file_path = "E:/Logistic_Project/SFTP_Files/" + file
        ftp.get(remote_file_path, local_file_path)
        print(f"Downloaded {file} to {local_file_path}")

    # Close the connection
    ftp.close()

except paramiko.AuthenticationException:
    print("Authentication failed, please verify your credentials")
except paramiko.SSHException as sshException:
    print(f"Unable to establish SSH connection: {sshException}")
except paramiko.ssh_exception.NoValidConnectionsError as e:
    print(f"Could not connect to {host}:{port}. Please check the port number.")
except Exception as e:
    print(f"Operation error: {e}")
finally:
    ssh_client.close()
    print("Connection closed.")
