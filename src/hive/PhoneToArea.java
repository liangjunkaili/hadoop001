package hive;

import java.util.HashMap;

import org.apache.hadoop.hive.ql.exec.UDF;

public class PhoneToArea extends UDF{

	private static HashMap<String, String> areaMap = new HashMap<>();
	static{
		areaMap.put("1388", "beijing");
		areaMap.put("1388", "beijing");
		areaMap.put("1388", "beijing");
	}
	public String evaluate(String phone){
		String result = areaMap.get(phone.substring(0, 4))==null?"huoxing":areaMap.get(phone.substring(0, 4));
		return result;
	}
}
