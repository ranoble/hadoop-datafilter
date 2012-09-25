package uk.co.tangentlabs.hadoop.output;
import java.io.DataOutputStream;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.ReflectionUtils;


public class JSONOutput extends FileOutputFormat<Text, MapWritable>{

		@Override
		public RecordWriter<Text, MapWritable> getRecordWriter(
				TaskAttemptContext job) throws IOException,
				InterruptedException {
			Configuration conf = job.getConfiguration();
			boolean isCompressed = getCompressOutput(job);
			CompressionCodec codec = null;
			String extension = "";
			if (isCompressed) {
				Class<? extends CompressionCodec> codecClass = 
						getOutputCompressorClass(job, GzipCodec.class);
				codec = (CompressionCodec) ReflectionUtils.newInstance(codecClass, conf);
				extension = codec.getDefaultExtension();
			}
			Path file = getDefaultWorkFile(job, extension);
			FileSystem fs = file.getFileSystem(conf);
			if (!isCompressed) {
				FSDataOutputStream fileOut = fs.create(file, false);
				return new JSONRecordWriter(fileOut);
			} else {
				FSDataOutputStream fileOut = fs.create(file, false);
				return new JSONRecordWriter(new DataOutputStream
						                                        (codec.createOutputStream(fileOut)));
			}
			
		}
		
	}
