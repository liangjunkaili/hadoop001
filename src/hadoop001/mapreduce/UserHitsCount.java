package hadoop001.mapreduce;

import java.io.IOException;
import java.util.List;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class UserHitsCount {

	// 输入文件路径
    public static final String INPUT_PATH = "hdfs://hadoop001:8020/input/qinmi.log";
    // 输出文件路径
    public static final String OUTPUT_PATH = "hdfs://hadoop001:8020/output/wordcount2";
	public static void main(String[] args) throws Exception {
		
		Configuration conf = new Configuration();
		try {
			Job job = Job.getInstance(conf, "UserHitsJob");
			job.setJarByClass(UserHitsCount.class);
			job.setMapperClass(UserHitsMapper.class);
//			job.setMapOutputKeyClass(Text.class);
//			job.setMapOutputValueClass(Text.class);
			
			job.setCombinerClass(UserHitsReduce.class);
			
			job.setReducerClass(UserHitsReduce.class);
			job.setOutputKeyClass(Text.class);
		    job.setOutputValueClass(IntWritable.class);
			
//			job.setInputFormatClass(UserHitsInput.class);
//			job.setOutputFormatClass(UserHitsOutput.class);
//			job.setPartitionerClass(UserHitsPartitioner.class);
			FileInputFormat.addInputPath(job, new Path(INPUT_PATH));
			FileOutputFormat.setOutputPath(job, new Path(OUTPUT_PATH));
			System.exit(job.waitForCompletion(true) ? 0 : 1);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	public static class UserHitsInput extends InputFormat<Object, Object>{

		@Override
		public List<org.apache.hadoop.mapreduce.InputSplit> getSplits(JobContext context)
				throws IOException, InterruptedException {
			return null;
		}

		@Override
		public org.apache.hadoop.mapreduce.RecordReader<Object, Object> createRecordReader(
				org.apache.hadoop.mapreduce.InputSplit split, TaskAttemptContext context)
				throws IOException, InterruptedException {
			return null;
		}

		
	}
	public static class UserHitsMapper extends Mapper<Object, Text, Text, IntWritable>{

		private static final IntWritable one = new IntWritable(1);
	    private Text word = new Text();
		@Override
		protected void map(Object key,Text value,Context context)
				throws IOException, InterruptedException {
			StringTokenizer itr = new StringTokenizer(value.toString());
		    while (itr.hasMoreTokens()) {
		      this.word.set(itr.nextToken());
		      context.write(this.word, one);
		    }
		}
		
	}
	public static class UserHitsPartitioner extends Partitioner<Object, Object>{
		@Override
		public int getPartition(Object key, Object value, int numPartitions) {
			return 0;
		}

	}
	public static class UserHitsReduce extends Reducer<Text, IntWritable, Text, IntWritable>{
		private IntWritable result = new IntWritable();
		@Override
		protected void reduce(Text key, Iterable<IntWritable> values, Context context)
				throws IOException, InterruptedException {
		      int sum = 0;
		      for (IntWritable val : values) {
		        sum += val.get();
		      }
		      this.result.set(sum);
		      context.write(key, this.result);
		}
	}
	public static class UserHitsOutput extends OutputFormat<Object, Object>{

		@Override
		public org.apache.hadoop.mapreduce.RecordWriter<Object, Object> getRecordWriter(TaskAttemptContext context)
				throws IOException, InterruptedException {
			return null;
		}

		@Override
		public void checkOutputSpecs(JobContext context) throws IOException, InterruptedException {
			
		}

		@Override
		public OutputCommitter getOutputCommitter(TaskAttemptContext context) throws IOException, InterruptedException {
			return null;
		}
		
	}
}
