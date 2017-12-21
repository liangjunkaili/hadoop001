package hadoop001.mapreduce;

import java.io.IOException;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class SortMR {

	public static class SortMapper extends Mapper<LongWritable, Text, FlowBean, NullWritable>{
		@Override
		protected void map(LongWritable key, Text value,
				Mapper<LongWritable, Text, FlowBean, NullWritable>.Context context)
				throws IOException, InterruptedException {
			String line =  value.toString();
			String[] fields = StringUtils.split(line,"\t");
			String phone = fields[0];
			long  u_flow = Long.parseLong(fields[1]);
			long d_flow = Long.parseLong(fields[2]);
			
			context.write(new FlowBean(phone,u_flow,d_flow), NullWritable.get());
		}
	}
	
	public static class SortReducer extends Reducer<FlowBean, NullWritable, Text, FlowBean>{
		@Override
		protected void reduce(FlowBean key, Iterable<NullWritable> value,
				Reducer<FlowBean, NullWritable, Text, FlowBean>.Context context)
				throws IOException, InterruptedException {
			String phone = key.getPhone();
			context.write(new Text(phone), key);
		}
	}
	
	public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
		
		Job job = Job.getInstance(conf);
		job.setJarByClass(SortMR.class);
		
		job.setMapperClass(SortMapper.class);
		job.setReducerClass(SortReducer.class);
		
		job.setMapOutputKeyClass(FlowBean.class);
		job.setMapOutputValueClass(NullWritable.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(FlowBean.class);
		
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		System.exit(job.waitForCompletion(true)?0:1);
	}
}
