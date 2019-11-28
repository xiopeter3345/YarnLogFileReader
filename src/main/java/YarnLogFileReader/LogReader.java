package YarnLogFileReader;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.Path;

import java.io.DataInputStream;

public interface LogReader {

    public void printContainerLogForFile(Path path, Configuration conf) throws Exception;

}
