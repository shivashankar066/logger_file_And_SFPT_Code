import pysftp

host = "sftp.tadl.com"
username = "TASL_Fedex@tadl.com"
password = "$E(urefTp@1608)&"
port = 443  # Change this to the correct port if needed

try:
    with pysftp.Connection(host, username=username, password=password, port=port) as sftp:
        print("Connection established successfully")
        remote_directory = "/TASL_Fedex/test/"
        files = sftp.listdir(remote_directory)
        print("Listing all the files and directories: ", files)

except Exception as e:
    print(f"Failed to connect: {e}")
