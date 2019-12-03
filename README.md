# Prerequisite
The program could be executed on any Linux machine. If it is running on cluster, the configuration will be read from configuration files, or you need to input information according to prompt. As the code needs to be compiled, maven needs to be installed on the machine.

Example:

On Ubuntu:

sudo apt-get install maven

# How to execute
1. Download all code

    git clone https://github.com/xiopeter3345/YarnLogFileReader.git

2. Grant execute permission to logreader.sh

    chmod +x logreader.sh

3. Run the program

    ./logreader.sh [path to the file]

# Example

1. You could pass full path as parameter.

    ./logreader.sh wasb://lazhuhadoop-2019-11-29t07-29-40-483z@lazhuhdi.blob.core.windows.net/app-logs/sshuser/logs/application_1575013613788_0001

2. You could pass relative path as parameter. The program will prompt to ask detailed information if it is not running on cluster. If it is running on cluster, it will treat root path as the value of fs.defaultFS.

    ./logreader.sh /app-logs/sshuser/logs/application_1575013613788_0001

# More Information

1. When running on cluster, you do not need to type any parameter. The program will try to read configuration from cluster. The root path will be the value of fs.defaultFS if you provide relative path.

2. For different type of storage, the following information needs to be provided if not running on cluster.

Blob Storage: Storage Key
Azure Datalake Gen 2: Storage kKey
Azure Datalake Gen 1: ClientId, ClientSecret, TenantId

3. If you could not get the following information, you could try to download the file from storage and print the log locally. When it asks for scheme, just type Enter will make the program to use file:// as scheme which will read local file.

4. For ESP cluster, when running on cluster, ADLS Gen1 and ADLS Gen2 still needs to do kinit as FileSystem needs to get user information. However, you could download the file to local path and run with scheme file://.

5. The program supports both TFile and IndexedFormat.

6. It does not support Windows now.

