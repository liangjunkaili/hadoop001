package hadoop001.mapreduce.kpi;

import java.text.ParseException;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class PageViewGroup extends WritableComparator{

	 public PageViewGroup() {
	        super(KPI.class, true);
	    }
	    @SuppressWarnings("rawtypes")
	    @Override
	    public int compare(WritableComparable a, WritableComparable b) {
	        KPI kpi1 = (KPI) a;
	        KPI kpi2 = (KPI) b;
	        try {
	            /**
	             * 判断当前bean与传入的bean的日期是否一致。如果一致则需要判断是否为同一个url
	             */
	            int i = kpi1.getTime_local_day().compareTo(kpi2.getTime_local_day());
	            if (i != 0) {
	                return -i;
	            }
	            return kpi1.getRequest().compareTo(kpi2.getRequest());
	        } catch (ParseException e) {
	            e.printStackTrace();
	        }
	        return 0;
	    }
}
