package personalProfile;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

import org.apache.hadoop.io.Text;


public class MyDate {
	
	public void nowDate() {
		Date now = new Date();
		SimpleDateFormat dateFormat = new SimpleDateFormat("yyyyMMdd");
//		System.out.println("today is " + dateFormat.format(now));
	}
	
	public static String getDay(int day) {
		Calendar calendar = Calendar.getInstance();
		SimpleDateFormat dateFormat = new SimpleDateFormat("yyyyMMdd");
		
		calendar.add(Calendar.DAY_OF_MONTH, day);
		return dateFormat.format(calendar.getTime());
	}
	
	public static String getLatestSunday() {
		Calendar calendar = Calendar.getInstance();

		calendar.add(Calendar.DATE, 0);
		calendar.set(Calendar.DAY_OF_WEEK, Calendar.SUNDAY);		
		return new SimpleDateFormat("yyyyMMdd").format(calendar.getTime());
	}
	
//	public String getUserId(Text value) {
//		String str = value.toString();
//		int startIndex = str.indexOf("\t");
//		int endIndex = str.indexOf("\t", startIndex + 1);
//		
//		System.out.println(startIndex + " " + endIndex + " " + str.substring(startIndex, endIndex));
//		return str.substring(startIndex, endIndex);
//	}
	
	public static void main(String[] args) {
		MyDate util = new MyDate();
		
		util.nowDate();
		util.getDay(-1);
//		util.getUserId(new Text("1000\t1000\t"));
	}
}
