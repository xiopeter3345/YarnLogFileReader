package YarnLogFileReader;

import org.apache.commons.lang3.SerializationUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.compress.Decompressor;
import org.apache.hadoop.io.file.tfile.BoundedRangeFileInputStream;
import org.apache.hadoop.io.file.tfile.Compression;
import org.apache.hadoop.yarn.logaggregation.filecontroller.ifile.LogAggregationIndexedFileController;

import java.io.*;
import java.util.*;

public class IndexedFormatLogReader implements LogReader {

    public void printContainerLogForFile(Path path, Configuration conf) throws Exception{

        Compression.Algorithm compressName = Compression.getCompressionAlgorithmByName("gz");
        Decompressor decompressor = compressName.getDecompressor();

        FileContext fileContext = FileContext.getFileContext(path.toUri(), conf);
        FSDataInputStream fsDataInputStream = fileContext.open(path);
        FSDataInputStream fsDataInputStream1 = fileContext.open(path);
        long fileLength = fileContext.getFileStatus(path).getLen();
        fsDataInputStream.seek(fileLength - 4L - 32L);
        int offset = fsDataInputStream.readInt();
        byte[] array = new byte[offset];
        fsDataInputStream.seek(fileLength - (long) offset - 4L - 32L);
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

        fsDataInputStream.close();
        fsDataInputStream1.close();

    }
}
