package hadoop001.mapreduce;

import java.io.IOException;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class WCMapper extends Mapper<LongWritable, Text, Text, LongWritable>{

	//mapreduce框架每读一次调用一次该方法
	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, LongWritable>.Context context)
			throws IOException, InterruptedException {
		//业务逻辑在此处实现
		String line = value.toString();
		String[] words = StringUtils.split(line, " ");
		for(String word : words){
			context.write(new Text(word), new LongWritable(1));
		}
	}

}
