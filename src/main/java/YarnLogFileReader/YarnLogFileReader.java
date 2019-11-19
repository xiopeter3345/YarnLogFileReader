package YarnLogFileReader;

import org.apache.commons.lang3.SerializationUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.compress.Decompressor;
import org.apache.hadoop.io.file.tfile.BoundedRangeFileInputStream;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.logaggregation.filecontroller.ifile.LogAggregationIndexedFileController;

import java.io.BufferedReader;
import java.io.EOFException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.*;

import org.apache.hadoop.io.file.tfile.Compression.Algorithm;
import org.apache.hadoop.io.file.tfile.Compression;

public class YarnLogFileReader
{

    private static List list = new ArrayList();

    public static void main( String[] args ) throws Exception
    {
        if(args.length != 1) {
            System.out.println("Usage: java -classpath <> YarnLogFileReader.YarnLogFileReader <full path of folder contains logs>" );
            System.out.println("Example: java -classpath '/usr/hdp/2.6.5.3008-11/hadoop/client/*:/usr/hdp/2.6.5.3008-11/hadoop/*:/home/sshuser/YarnLogFileReader/target/YarnLogFileReader-1.0-SNAPSHOT.jar:.:/etc/hadoop/conf' YarnLogFileReader.YarnLogFileReader wasb://lazhuhdi-2019-05-09t07-12-39-811z@lzlazhuhdi.blob.core.windows.net//app-logs/chenghao.guo/logs-ifile/application_1557457099458_0010");
            System.exit(1);
        }

        YarnLogFileReader app = new YarnLogFileReader();
        app.printAllContainerLog(args[0]);
    }

    public void printAllContainerLog(String file) throws Exception {
        Configuration conf = new YarnConfiguration();

        // workaround iocache
        if (conf.get("fs.wasb.impl") == null){
            conf.set("fs.AbstractFileSystem.wasb.impl", "fs.AbstractFileSystem.wasb.impl");
            conf.set("fs.AbstractFileSystem.wasbs.impl", "org.apache.hadoop.fs.azure.Wasbs");
            conf.set("fs.wasb.impl", "org.apache.hadoop.fs.azure.NativeAzureFileSystem");
            conf.set("fs.wasbs.impl", "org.apache.hadoop.fs.azure.NativeAzureFileSystem");
        }
        List result = getAllFiles(new Path(file), conf);
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
                    sb.append("LogLastModifiedTime: " + new Date(indexedFileLogMeta.getLastModificatedTime()) + "\n");
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

    private List getAllFiles(Path path, Configuration conf) throws Exception {

        FileSystem fs = FileSystem.newInstance(conf);
        if (!fs.getFileStatus(path).isDirectory())
            list.add(path);
        else {
            FileStatus[] files = fs.listStatus(path);
            for(int i = 0; i < files.length; i++){
                if(files[i].isDirectory())
                    getAllFiles(files[i].getPath(), conf);
                else
                    list.add(files[i].getPath());
            }
        }
        return list;
    }

}
