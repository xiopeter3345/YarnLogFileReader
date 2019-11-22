package YarnLogFileReader;

import org.apache.commons.lang3.SerializationUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.compress.Decompressor;
import org.apache.hadoop.io.file.tfile.BoundedRangeFileInputStream;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.logaggregation.filecontroller.ifile.LogAggregationIndexedFileController;

import java.io.*;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.*;

import org.apache.hadoop.io.file.tfile.Compression.Algorithm;
import org.apache.hadoop.io.file.tfile.Compression;

import java.security.MessageDigest;
import org.apache.commons.codec.binary.Base64;
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
        /*if(args.length != 1) {
            System.out.println("Usage: java -classpath <> YarnLogFileReader.YarnLogFileReader <full path of folder contains logs>" );
            System.out.println("Example: java -classpath '/usr/hdp/2.6.5.3008-11/hadoop/client/*:/usr/hdp/2.6.5.3008-11/hadoop/*:/home/sshuser/YarnLogFileReader/target/YarnLogFileReader-1.0-SNAPSHOT.jar:.:/etc/hadoop/conf' YarnLogFileReader.YarnLogFileReader wasb://lazhuhdi-2019-05-09t07-12-39-811z@lzlazhuhdi.blob.core.windows.net//app-logs/chenghao.guo/logs-ifile/application_1557457099458_0010");
            System.exit(1);
        }

        YarnLogFileReader app = new YarnLogFileReader();
        app.printAllContainerLog(args[0]);*/
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

            BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));

            int schemeIndex;
            if ((schemeIndex = path.indexOf("://")) != -1) {
                String scheme = path.substring(0, schemeIndex);
                conf.set("fs.defaultFS", path.substring(0, path.indexOf("/", schemeIndex+3)));
                switch (scheme) {
                    case "wasb":
                    case "wasbs":
                    case "abfs":
                    case "abfss":
                        if("wasb".equals(scheme) || "wasbs".equals("scheme"))
                            System.out.println("Scheme is blob storage");
                        else
                            System.out.println("Scheme is adls gen2");
                        System.out.print("Storage key:");
                        String storageKey = reader.readLine();
                        String accountName = path.substring(path.indexOf("@")+1, path.indexOf("/", schemeIndex+3));
                        conf.set("fs.azure.account.key."+accountName, storageKey);
                        break;

                    case "adl":
                    case "adls":
                        System.out.println("Scheme is adls gen1");

                        System.out.print("Client ID:");
                        String clientId = reader.readLine();
                        System.out.print("Client Secret:");
                        String clientSecret = reader.readLine();
                        System.out.print("Tenant ID:");
                        String tenantId = reader.readLine();

                        conf.set("dfs.adls.oauth.access.token.provider.type", "ClientCredential");
                        conf.set("dfs.adls.oauth2.refresh.url", "https://login.microsoftonline.com/"+tenantId+"/oauth2/token");
                        conf.set("dfs.adls.oauth2.client.id", clientId);
                        conf.set("dfs.adls.oauth2.credential", clientSecret);
                        break;

                    default:

                        System.out.println("not supported scheme " + scheme);
                }
            } else {

                System.out.print("Type scheme (wasb, wasbs, abfs, abfss, adl, adls):");
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
                        System.out.print("Account Name (short name):");
                        String accountName = reader.readLine();
                        System.out.print("Container Name:");
                        String containerName = reader.readLine();
                        System.out.print("Storage key:");
                        String storageKey = reader.readLine();

                        if("wasb".equals(scheme) || "wasbs".equals(scheme)) {
                            conf.set("fs.defaultFS", scheme + "://" + containerName + "@" + accountName + ".blob.core.windows.net");
                            conf.set("fs.azure.account.key." + accountName + ".blob.core.windows.net", storageKey);
                        } else {
                            conf.set("fs.defaultFS", scheme + "://" + containerName + "@" + accountName + ".dfs.core.windows.net");
                            conf.set("fs.azure.account.key." + accountName + ".dfs.core.windows.net", storageKey);
                        }

                        break;
                    case "adl":
                    case "adls":

                        System.out.println("Scheme is adls gen1");

                        System.out.print("Account Name (short name):");
                        String adlsAccountName = reader.readLine();
                        System.out.print("Client ID:");
                        String clientId = reader.readLine();
                        System.out.print("Client Secret:");
                        String clientSecret = reader.readLine();
                        System.out.print("Tenant ID:");
                        String tenantId = reader.readLine();

                        conf.set("fs.defaultFS", scheme + "://" + adlsAccountName + ".azuredatalakestore.net");
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

    public void printAllContainerLog(String file) throws Exception {

        List result = getAllFiles(new Path(file));
        for(int i = 0; i < result.size(); i++) {
            printContainerLogForFile((Path) result.get(i), conf);
        }
    }

    private void printContainerLogForFile(Path path, Configuration conf) throws Exception {
        Algorithm compressName = Compression.getCompressionAlgorithmByName("gz");
        Decompressor decompressor = compressName.getDecompressor();

        FileContext fileContext = FileContext.getFileContext(path.toUri(), conf);
        FSDataInputStream fsDataInputStream = fileContext.open(path);
        FSDataInputStream fsDataInputStream1 = fileContext.open(path);
        long fileLength = fileContext.getFileStatus(path).getLen();
        try {
            fsDataInputStream.seek(fileLength - 4L - 32L);
        } catch (EOFException ex) {
            System.out.printf("The file %s not an indexed formatted log file", path.toString());
            return;
        }
        int offset = fsDataInputStream.readInt();
        byte[] array = new byte[offset];
        try {
            fsDataInputStream.seek(fileLength - (long) offset - 4L - 32L);
        } catch (EOFException ex) {
            System.out.printf("The file %s is not an indexed formatted log file", path.toString());
            return;
        }
        int actual = fsDataInputStream.read(array);

        LogAggregationIndexedFileController.IndexedLogsMeta logMeta = (LogAggregationIndexedFileController.IndexedLogsMeta) SerializationUtils.deserialize(array);
        Iterator iter = logMeta.getLogMetas().iterator();
        while(iter.hasNext()) {
            LogAggregationIndexedFileController.IndexedPerAggregationLogMeta perAggregationLogMeta = (LogAggregationIndexedFileController.IndexedPerAggregationLogMeta) iter.next();
            Iterator iter1 = new TreeMap(perAggregationLogMeta.getLogMetas()).entrySet().iterator();
            while(iter1.hasNext()) {
                Map.Entry<String, List<LogAggregationIndexedFileController.IndexedFileLogMeta>> log = (Map.Entry) iter1.next();
                Iterator iter2 = log.getValue().iterator();
                InputStream in = null;
                while(iter2.hasNext()) {
                    LogAggregationIndexedFileController.IndexedFileLogMeta indexedFileLogMeta = (LogAggregationIndexedFileController.IndexedFileLogMeta) iter2.next();
                    in = compressName.createDecompressionStream(new BoundedRangeFileInputStream(fsDataInputStream1, indexedFileLogMeta.getStartIndex(), indexedFileLogMeta.getFileCompressedSize()), decompressor, 262144);
                    StringBuilder sb = new StringBuilder();
                    String containerStr = String.format("Container: %s on %s", indexedFileLogMeta.getContainerId(), path.getName());
                    sb.append(containerStr + "\n");
                    sb.append("LogType: " + indexedFileLogMeta.getFileName() + "\n");
                    sb.append("LogLastModifiedTime: " + new Date(indexedFileLogMeta.getLastModifiedTime()) + "\n");
                    sb.append("LogLength: " + indexedFileLogMeta.getFileSize() + "\n");
                    sb.append("LogContents:\n");
                    BufferedReader br = new BufferedReader(new InputStreamReader(in));
                    System.out.println(sb.toString());
                    String line = null;
                    while((line = br.readLine()) != null) {
                        System.out.println(line);
                    }

                    System.out.printf("End of LogType: %s\n", indexedFileLogMeta.getFileName());
                    System.out.printf("*****************************************************************************\n\n");
                }
            }
        }
    }

    private List getAllFiles(Path path) throws Exception {

        FileSystem fs = FileSystem.newInstance(conf);
        if (!fs.getFileStatus(path).isDirectory())
            list.add(path);
        else {
            FileStatus[] files = fs.listStatus(path);
            for(int i = 0; i < files.length; i++){
                if(files[i].isDirectory())
                    getAllFiles(files[i].getPath());
                else
                    list.add(files[i].getPath());
            }
        }
        return list;
    }

}
