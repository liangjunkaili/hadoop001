package hadoop001.mapreduce;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Partitioner;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;

public class UserHitsCount {

	public static void main(String[] args) {
		
	}
	public static class UserHitsInput implements InputFormat<Object, Object>{

		@Override
		public RecordReader<Object, Object> getRecordReader(InputSplit arg0, JobConf arg1, Reporter arg2)
				throws IOException {
			return null;
		}

		@Override
		public InputSplit[] getSplits(JobConf arg0, int arg1) throws IOException {
			return null;
		}
		
	}
	public static class UserHitsMapper implements Mapper<Object, Text, Text, IntWritable>{

		@Override
		public void configure(JobConf arg0) {
			
		}

		@Override
		public void close() throws IOException {
			
		}

		@Override
		public void map(Object arg0, Text arg1, OutputCollector<Text, IntWritable> arg2, Reporter arg3)
				throws IOException {
			
		}
		
	}
	public static class UserHitsPartitioner implements Partitioner<Object, Object>{

		@Override
		public void configure(JobConf arg0) {
			
		}

		@Override
		public int getPartition(Object arg0, Object arg1, int arg2) {
			return 0;
		}
		
	}
}
