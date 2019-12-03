package YarnLogFileReader;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.logaggregation.AggregatedLogFormat;
import org.apache.hadoop.yarn.logaggregation.ContainerLogAggregationType;
import org.apache.hadoop.yarn.logaggregation.LogToolUtils;
import org.apache.hadoop.yarn.util.Times;

import java.io.DataInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.Charset;

public class TFileLogReader implements LogReader {

    @Override
    public void printContainerLogForFile(Path path, Configuration conf) throws Exception {

        try {
            AggregatedLogFormat.LogReader reader = new AggregatedLogFormat.LogReader(conf, path);
            AggregatedLogFormat.LogKey key = new AggregatedLogFormat.LogKey();
            FileContext context = FileContext.getFileContext(path.toUri(), conf);
            FileStatus status = context.getFileStatus(path);
            long size = context.getFileStatus(path).getLen();
            byte[] buf = new byte['\uffff'];

            DataInputStream valueStream = reader.next(key);
            while (true) {
                try {
                    String fileType = valueStream.readUTF();
                    String fileLengthStr = valueStream.readUTF();
                    long fileLength = Long.parseLong(fileLengthStr);
                    LogToolUtils.outputContainerLog(key.toString(), path.getName(), fileType, fileLength, size, Times.format(status.getModificationTime()), valueStream, (OutputStream) System.out, buf, ContainerLogAggregationType.AGGREGATED);
                    byte[] b = this.aggregatedLogSuffix(fileType).getBytes(Charset.forName("UTF-8"));
                    ((OutputStream) System.out).write(b, 0, b.length);
                } catch (EOFException eofException) {
                    break;
                }
            }
        }catch(IOException ioe) {
            if("Not a valid BCFile."
                    .equals(ioe.getMessage())) {
                return;
            } else
                throw ioe;
        }

    }

    private String aggregatedLogSuffix(String fileName) {
        StringBuilder sb = new StringBuilder();
        String endOfFile = "End of LogType:" + fileName;
        sb.append("\n" + endOfFile + "\n");
        sb.append(StringUtils.repeat("*", endOfFile.length() + 50) + "\n\n");
        return sb.toString();
    }

}
