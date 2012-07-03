import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Mapper;

public class IVisitWhoMapper extends Mapper<LongWritable, Text, Text, Text> {
	private static final long HALFYEAR = (long)60*60*24*1000*180;
	private static final long dayUnit = 24 * 3600 * 1000;
	private static final long now = new Date().getTime();
	
	public void map(LongWritable key, Text values, Context context)
			throws IOException, InterruptedException {
		String line = values.toString().toLowerCase();
		List<String> fields = new ArrayList<String>();
		SimpleStringTokenizer simpleTokenizer = new SimpleStringTokenizer(line, "\t");
		fields = simpleTokenizer.getAllElements();
		//取最近半年的
		try {
			SimpleDateFormat df = new SimpleDateFormat("yy-MM-dd HH:mm:ss"); 
			Date gmt_create = df.parse(fields.get(7));
			if(new Date().getTime() - gmt_create.getTime() > HALFYEAR ){
				return;
			}
		} catch (Exception e) {
			return;
		}
		Date d = null;
		try {
			SimpleDateFormat sdf = new SimpleDateFormat("yy-MM-dd");
			d = sdf.parse(fields.get(7).substring(0,10));
		} catch (Exception e) {
			return;
		}
		context.write(new Text(fields.get(5)), new Text(fields.get(6) + "\t" + (now-d.getTime())/dayUnit ));
	}
	
	public static void main(String[] args) {

	}
}
