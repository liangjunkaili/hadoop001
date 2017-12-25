package hadoop001.mapreduce.kpi;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashSet;
import java.util.Locale;
import java.util.Set;

import org.apache.hadoop.io.WritableComparable;

public class KPI implements WritableComparable<KPI>{

	private String remote_addr;// 来访ip
    private String remote_user;// 来访用户名称，忽略属性“-”
    private String time_local;// 记录时间与时区
    private String request;// 访问页面
    private String status;// 返回状态
    private String body_bytes_sent;// 返回客户端内容主体大小
    private String http_referer;// 来访页面
    private String http_user_agent;// 客户浏览器的相关信息
    private boolean valid = true;// 检验数据是否合法
    
    //设置需要统计的页面类型
    static Set<String> pages = null;
    static{
    	pages = new HashSet<>();
    	pages.add(".php");
    }
    private static KPI parser(String line){
    	KPI kpi = new KPI();
    	String arr[] = line.split(" ");
    	if(arr.length>11){
    		 kpi.setRemote_addr(arr[0]);
             kpi.setRemote_user(arr[1]);
             kpi.setTime_local(arr[3].substring(1));
             String url = arr[6].indexOf("?") != -1 ? arr[6].substring(0, arr[6].indexOf("?")) : arr[6];
             kpi.setRequest(url);
             kpi.setStatus(arr[8]);
             kpi.setBody_bytes_sent(arr[9]);
             kpi.setHttp_referer(arr[10]);
             if (arr.length > 12) {
                 kpi.setHttp_user_agent(arr[11] + " " + arr[12]);
             } else {
                 kpi.setHttp_user_agent(arr[11]);
             }
             if (Integer.parseInt(kpi.getStatus()) >= 400) {
                 kpi.setValid(false);
             }
    	}else{
    		kpi.setValid(false);
    	}
    	return kpi;
    }
    public static KPI filterPVs(String line) {
        KPI kpi = parser(line);
        kpi.setValid(false);
        for (String page : pages) {
            if (kpi.getRequest() != null) {
                if (kpi.getRequest().contains(page)) {
                    kpi.setValid(true);
                    break;
                }
            }
        }
        return kpi;
    }
	@Override
	public void write(DataOutput out) throws IOException {
		out.writeUTF(this.getRemote_addr());
        out.writeUTF(this.getRemote_user());
        out.writeUTF(this.getTime_local());
        out.writeUTF(this.getRequest());
        out.writeUTF(this.getStatus());
        out.writeUTF(this.getBody_bytes_sent());
        out.writeUTF(this.getHttp_referer());
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		this.setRemote_addr(in.readUTF());
        this.setRemote_user(in.readUTF());
        this.setTime_local(in.readUTF());
        this.setRequest(in.readUTF());
        this.setStatus(in.readUTF());
        this.setBody_bytes_sent(in.readUTF());
        this.setHttp_referer(in.readUTF());
	}

	@Override
	public int compareTo(KPI o) {
		 int i;
	        try {
	            i = this.getTime_local_day().compareTo(o.getTime_local_day());
	            if (i != 0) {
	                return -i;
	            }
	        } catch (ParseException e) {
	            e.printStackTrace();
	        }
	        return this.getRequest().compareTo(o.getRequest());
	}
	public String getRemote_addr() {
		return remote_addr;
	}
	public void setRemote_addr(String remote_addr) {
		this.remote_addr = remote_addr;
	}
	public String getRemote_user() {
		return remote_user;
	}
	public void setRemote_user(String remote_user) {
		this.remote_user = remote_user;
	}
	public String getTime_local() {
		return time_local;
	}
	  public Date getTime_local_Date() {
	        SimpleDateFormat df = new SimpleDateFormat("dd/MMM/yyyy:HH:mm:ss", Locale.US);
	        try {
	            return df.parse(this.time_local);
	        } catch (ParseException e) {
	            return null;
	        }
	    }
	    public String getTime_local_day() throws ParseException {
	        SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd");
	        return df.format(this.getTime_local_Date());
	    }
	    public String getTime_local_Date_hour() throws ParseException {
	        SimpleDateFormat df = new SimpleDateFormat("yyyyMMddHH");
	        return df.format(this.getTime_local_Date());
	    }
	public void setTime_local(String time_local) {
		this.time_local = time_local;
	}
	public String getRequest() {
		return request;
	}
	public void setRequest(String request) {
		this.request = request;
	}
	public String getStatus() {
		return status;
	}
	public void setStatus(String status) {
		this.status = status;
	}
	public String getBody_bytes_sent() {
		return body_bytes_sent;
	}
	public void setBody_bytes_sent(String body_bytes_sent) {
		this.body_bytes_sent = body_bytes_sent;
	}
	public String getHttp_referer() {
		return http_referer;
	}
	public void setHttp_referer(String http_referer) {
		this.http_referer = http_referer;
	}
	public String getHttp_user_agent() {
		return http_user_agent;
	}
	public void setHttp_user_agent(String http_user_agent) {
		this.http_user_agent = http_user_agent;
	}
	public boolean isValid() {
		return valid;
	}
	public void setValid(boolean valid) {
		this.valid = valid;
	}
}
