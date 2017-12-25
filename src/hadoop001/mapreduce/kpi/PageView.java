package hadoop001.mapreduce.kpi;

import java.io.IOException;
import java.text.ParseException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class PageView {

	 /**
     * 利用mapreduce 离线统计每天的pv
     * 
     * @author itunic
     *
     */
    public static class PageVisitsMapper extends Mapper<LongWritable, Text, KPI, LongWritable> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            // 校验每一行URL的合法性
            KPI kpi = KPI.filterPVs(value.toString());
            if (kpi.isValid()) {
                // 利用mapper特性输出给reducer
                context.write(kpi, new LongWritable(1));
            }
        }
    }
    public static class PageVisitsReducer extends Reducer<KPI, LongWritable, Text, Text> {
        @Override
        protected void reduce(KPI key, Iterable<LongWritable> value, Context context)
                throws IOException, InterruptedException {
            long count = 0;
            // 将相同的分组相同的页面循环叠加
            for (LongWritable l : value) {
                count += l.get();
            }
            String out = key.getRequest() + "\t" + count;
            try {
                context.write(new Text(key.getTime_local_day()), new Text(out));
            } catch (ParseException e) {
                e.printStackTrace();
            }
        }
    }
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);
        job.setJarByClass(PageView.class);
        job.setMapperClass(PageVisitsMapper.class);
        job.setReducerClass(PageVisitsReducer.class);
        job.setMapOutputKeyClass(KPI.class);
        job.setMapOutputValueClass(LongWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setGroupingComparatorClass(PageViewGroup.class);
        FileInputFormat.setInputPaths(job, new Path("F:\\test\\input\\access.log.fensi"));
        FileOutputFormat.setOutputPath(job, new Path("F:\\test\\output9"));
        int i = job.waitForCompletion(true) ? 0 : 1;
        System.exit(i);
    }
}
