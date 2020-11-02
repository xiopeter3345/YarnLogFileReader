package YarnLogFileReader;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.fs.azure.AzureException;
import org.apache.hadoop.io.compress.Decompressor;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.logaggregation.AggregatedLogFormat;

import java.io.*;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.*;

import org.apache.hadoop.io.file.tfile.Compression.Algorithm;
import org.apache.hadoop.io.file.tfile.Compression;

import java.security.SecureRandom;

public class YarnLogFileReader
{

    private static List list = new ArrayList();

    private static Configuration conf = new YarnConfiguration();

    private static final SecureRandom RAN = new SecureRandom();

    static {
        conf.set("fs.AbstractFileSystem.wasb.impl", "org.apache.hadoop.fs.azure.Wasb");
        conf.set("fs.AbstractFileSystem.wasbs.impl", "org.apache.hadoop.fs.azure.Wasbs");
        conf.set("fs.wasb.impl", "org.apache.hadoop.fs.azure.NativeAzureFileSystem");
        conf.set("fs.wasbs.impl", "org.apache.hadoop.fs.azure.NativeAzureFileSystem");
        conf.set("fs.abfs.impl", "org.apache.hadoop.fs.azurebfs.AzureBlobFileSystem");
        conf.set("fs.abfss.impl", "org.apache.hadoop.fs.azurebfs.SecureAzureBlobFileSystem");
        conf.set("fs.AbstractFileSystem.abfs.impl", "org.apache.hadoop.fs.azurebfs.Abfs");
        conf.set("fs.AbstractFileSystem.abfss.impl", "org.apache.hadoop.fs.azurebfs.Abfss");
    }

    public static void main( String[] args ) throws Exception
    {
        if(args.length != 1) {
            System.out.println("Usage: java -classpath '/etc/hadoop/conf:./target/YarnLogFileReader-1.0-SNAPSHOT-dependencies.jar:/usr/hdp/current/hadoop-hdfs-client/lib/adls2-oauth2-token-provider.jar' YarnLogFileReader.YarnLogFileReader <path of folder contains logs>" );
            System.out.println("Example: java -classpath '/etc/hadoop/conf:./target/YarnLogFileReader-1.0-SNAPSHOT-dependencies.jar:/usr/hdp/current/hadoop-hdfs-client/lib/adls2-oauth2-token-provider.jar' YarnLogFileReader.YarnLogFileReader wasb://lazhuhdi-2019-05-09t07-12-39-811z@lzlazhuhdi.blob.core.windows.net//app-logs/chenghao.guo/logs-ifile/application_1557457099458_0010");
            System.exit(1);
        }

        try {
            InetAddress headnodehost = InetAddress.getByName("headnodehost");
        } catch(UnknownHostException ex) {
            System.out.println("Not running on cluster");
            conf.set("fs.adl.impl", "org.apache.hadoop.fs.adl.AdlFileSystem");
            conf.set("fs.adls.impl", "org.apache.hadoop.fs.adl.AdlFileSystem");
            conf.set("fs.AbstractFileSystem.adl.impl", "org.apache.hadoop.fs.adl.Adl");
            conf.set("fs.AbstractFileSystem.adls.impl", "org.apache.hadoop.fs.adl.Adl");
            YarnLogFileReader app = new YarnLogFileReader(false, args[0]);
            app.printAllContainerLog(args[0]);
            System.exit(0);
        }

        YarnLogFileReader app = new YarnLogFileReader(true, "");
        app.printAllContainerLog(args[0]);

    }

    public YarnLogFileReader(boolean isCluster, String path) throws IOException {

        if (!isCluster) {

            Console console = System.console();
            BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));

            int schemeIndex;
            if ((schemeIndex = path.indexOf("://")) != -1) {
                String scheme = path.substring(0, schemeIndex);
                
                switch (scheme) {
                    case "wasb":
                    case "wasbs":
                    case "abfs":
                    case "abfss":
                        if("wasb".equals(scheme) || "wasbs".equals("scheme"))
                            System.out.println("Scheme is blob storage");
                        else
                            System.out.println("Scheme is adls gen2");
                        String accountName = path.substring(path.indexOf("@")+1, path.indexOf("/", schemeIndex+3));
                        System.out.printf("Storage key (%s):", accountName);
                        char[] storageKeyChars = console.readPassword();
                        String storageKey = new String(storageKeyChars);

                        conf.set("fs.azure.account.key."+accountName, storageKey);
                        conf.set("fs.defaultFS", path.substring(0, path.indexOf("/", schemeIndex+3)));
                        break;

                    case "adl":
                        System.out.println("Scheme is adls gen1");

                        String adlsAccountName = path.substring(schemeIndex+3, path.indexOf("/", schemeIndex+3));

                        System.out.printf("Client ID (%s): ", adlsAccountName);
                        String clientId = reader.readLine();
                        System.out.printf("Client Secret (%s): ", adlsAccountName);
                        char[] clientSecretChars = console.readPassword();
                        String clientSecret = new String(clientSecretChars);
                        System.out.printf("Tenant ID (%s): ", adlsAccountName);
                        String tenantId = reader.readLine();

                        conf.set("dfs.adls.oauth.access.token.provider.type", "ClientCredential");
                        conf.set("dfs.adls.oauth2.refresh.url", "https://login.microsoftonline.com/"+tenantId+"/oauth2/token");
                        conf.set("dfs.adls.oauth2.client.id", clientId);
                        conf.set("dfs.adls.oauth2.credential", clientSecret);
                        conf.set("fs.defaultFS", path.substring(0, path.indexOf("/", schemeIndex+3)));
                        break;

                    default:
                        conf.set("fs.defaultFS", "file:///");
                        conf.set("fs.AbstractFileSystem.file.impl", "org.apache.hadoop.fs.local.LocalFs");
                        System.out.println("Try local file system");
                }
            } else {

                System.out.print("Type scheme (wasb, wasbs, abfs, abfss, adl):");
                String scheme = reader.readLine();

                switch (scheme) {
                    case "wasb":
                    case "wasbs":
                    case "abfs":
                    case "abfss":

                        if("wasb".equals(scheme) || "wasbs".equals(scheme))
                            System.out.println("Scheme is blob storage");
                        else
                            System.out.println("Scheme is adls gen2");
                        System.out.print("Storage Account Name:");
                        String accountName = reader.readLine();
                        accountName = resolveAccountName(accountName, scheme);
                        System.out.printf("Container Name (%s): ", accountName);
                        String containerName = reader.readLine();
                        System.out.printf("Storage key (%s): ", accountName);
                        char[] storageKeyChars = console.readPassword();
                        String storageKey = new String(storageKeyChars);

                        if("wasb".equals(scheme) || "wasbs".equals(scheme)) {
                            conf.set("fs.defaultFS", scheme + "://" + containerName + "@" + accountName);
                            conf.set("fs.azure.account.key." + accountName, storageKey);
                        } else {
                            conf.set("fs.defaultFS", scheme + "://" + containerName + "@" + accountName);
                            conf.set("fs.azure.account.key." + accountName, storageKey);
                        }

                        break;
                    case "adl":
                    case "adls":

                        System.out.println("Scheme is adls gen1");

                        System.out.print("Data Lake Account Name:");
                        String adlsAccountName = reader.readLine();
                        adlsAccountName = resolveAccountName(adlsAccountName, scheme);
                        System.out.printf("Client ID (%s): ", adlsAccountName);
                        String clientId = reader.readLine();
                        System.out.printf("Client Secret (%s): ", adlsAccountName);
                        char[] clientSecretChars = console.readPassword();
                        String clientSecret = new String(clientSecretChars);
                        System.out.printf("Tenant ID (%s): ", adlsAccountName);
                        String tenantId = reader.readLine();

                        conf.set("fs.defaultFS", scheme + "://" + adlsAccountName);
                        conf.set("dfs.adls.oauth.access.token.provider.type", "ClientCredential");
                        conf.set("dfs.adls.oauth2.refresh.url", "https://login.microsoftonline.com/"+tenantId+"/oauth2/token");
                        conf.set("dfs.adls.oauth2.client.id", clientId);
                        conf.set("dfs.adls.oauth2.credential", clientSecret);

                        break;

                    default:

                        conf.set("fs.defaultFS", "file:///");

                }
            }
        }
    }

    private String resolveAccountName(String accountName, String scheme) {

        if(accountName.indexOf(".") != -1)
            accountName = accountName.substring(0, accountName.indexOf("."));
        switch(scheme) {
            case "wasb":
            case "wasbs":
                accountName += ".blob.core.windows.net";
                break;
            case "abfs":
            case "abfss":
                accountName += ".dfs.core.windows.net";
                break;
            case "adl":
                accountName += ".azuredatalakestore.net";
                break;
        }

        return accountName;
    }

    private void printAllContainerLog(String file) throws Exception {

        List result = getAllFiles(new Path(file));
        if(result.size() == 0) {
            System.out.println("No file found");
            System.exit(0);
        }
        for(int i = 0; i < result.size(); i++) {
            printContainerLogForFile((Path) result.get(i));
        }
    }

    private void printContainerLogForFile(Path path) throws Exception {
        Algorithm compressName = Compression.getCompressionAlgorithmByName("gz");
        Decompressor decompressor = compressName.getDecompressor();

        try {
            LogReader logReader = probeFileFormat(path);

            logReader.printContainerLogForFile(path, conf);
        }catch(Exception ex){
            return;
        }
    }

    private LogReader probeFileFormat(Path path) throws Exception {
        FileContext fileContext = FileContext.getFileContext(path.toUri(), conf);
        FSDataInputStream fsDataInputStream = fileContext.open(path);
        long fileLength = fileContext.getFileStatus(path).getLen();
        try {
            fsDataInputStream.seek(fileLength - 4L - 32L);
            int offset = fsDataInputStream.readInt();
            if(offset >= 10485760)
                    throw new Exception();
            byte[] array = new byte[offset];
            fsDataInputStream.seek(fileLength - (long) offset - 4L - 32L);
            fsDataInputStream.close();
            return new IndexedFormatLogReader();
        } catch (Exception eofex) {

            try {
                AggregatedLogFormat.LogReader reader = new AggregatedLogFormat.LogReader(conf, path);
                return new TFileLogReader();
            } catch(Exception ex) {
                System.out.printf("The file %s is not an indexed formatted log file\n", path.toString());
                throw ex;
            }
        }
    }

    private List getAllFiles(Path path) throws Exception {

        try {
            FileSystem fs = FileSystem.newInstance(conf);
            if (!fs.getFileStatus(path).isDirectory())
                list.add(path);
            else {
                FileStatus[] files = fs.listStatus(path);
                for (int i = 0; i < files.length; i++) {
                    if (files[i].isDirectory())
                        getAllFiles(files[i].getPath());
                    else
                        list.add(files[i].getPath());
                }
            }
            return list;
        } catch (AzureException ex) {
            System.out.println("Unable to initialize the filesystem or unable to list file status, please check input parameters");
            throw ex;
        }
    }

}
