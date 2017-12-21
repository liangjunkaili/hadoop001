package hadoop001.mapreduce;

import java.io.IOException;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class FlowNumMapper extends Mapper<LongWritable, Text, Text, FlowBean>{

	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, FlowBean>.Context context)
			throws IOException, InterruptedException {
		String line = value.toString();
		String[] fields = StringUtils.split(line, "\t");
		String phone = fields[1];
		long u_flow = Long.parseLong(fields[7]);
		long d_flow = Long.parseLong(fields[8]);
		
		context.write(new Text(phone), new FlowBean(phone,u_flow,d_flow));
	}
}
