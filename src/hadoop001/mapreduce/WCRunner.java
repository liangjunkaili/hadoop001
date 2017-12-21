package hadoop001.mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class WCRunner {

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		
		conf.set("mapreduce.job.jar", "wc.jar");
		Job job = Job.getInstance(conf );
		
		job.setJarByClass(WCRunner.class);
		
		job.setMapperClass(WCMapper.class);
		job.setReducerClass(WCReducer.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(LongWritable.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(LongWritable.class);
		
		FileInputFormat.setInputPaths(job, new Path("/wc/srcdata/"));
		
		FileOutputFormat.setOutputPath(job, new Path("/wc/output/"));
		
		job.waitForCompletion(true);
	}
}
