import java.io.IOException;
import java.text.BreakIterator;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.StringTokenizer;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Mapper;

public class CommentMapper extends Mapper<LongWritable, Text, Text, Text> {
	private static final long HALFYEAR = (long)60*60*24*1000*180;
	
	public void map(LongWritable key, Text values, Context context)
			throws IOException, InterruptedException {
		String line = values.toString().toLowerCase();
		List<String> fields = new ArrayList<String>();
		SimpleStringTokenizer simpleTokenizer = new SimpleStringTokenizer(line, "\t", 22);
		fields = simpleTokenizer.getAllElements();
		//ȡ��������
		try {
			SimpleDateFormat df = new SimpleDateFormat("yy-MM-dd HH:mm:ss"); 
			Date gmt_create = df.parse(fields.get(15));
			if(new Date().getTime() - gmt_create.getTime() > HALFYEAR ){
				return;
			}
		} catch (Exception e) {
			return;
		}
		
		// ������ƹ�˵������,�Ͳ�Ҫ��.
		if("11".equals(fields.get(10)))
			return;

		
//		// ɾ���Ĳ�Ҫ
//		if("1".equals(fields.get(20)))
//			return;
		
		String userId = fields.get(3);
		String content = fields.get(7);
		content = content
				.replaceAll(
						"(@[\\S&&[^\\p{Punct}]]+((\\({1}[\\S&&[^\\p{Punct}]]+\\,{1}\\d+\\){1})|(\\[{1}[\\S&&[^\\p{Punct}]]+\\,{1}\\d+\\]{1})))|^(((���){1}|(����){1}|(�ظ�){1})([\\S&&[^\\p{Punct}]&&[^\\p{Space}]&&[^\\uFF00-\\uFFEF]]){1,10})|([\\S&&[^\\p{Punct}]&&[^\\p{Space}]&&[^\\uFF00-\\uFFEF]]{1,10}((���){1}|(����){1}))|(#{1}[^#]*#{1})",
						"��");
		StringBuilder sb = new StringBuilder();
		for (int i = 0; i < content.length(); i++) {
			char ch = content.charAt(i);
			if (isAvalidChar(ch)) {
				sb.append(ch);
			}
		}
		content = sb.toString();
		if (StringUtils.isEmpty(content)) {
			return;
		}
		context.getCounter("comment ref", "all ok").increment(1);
//		fields.set(7, content);
		Text outKey = new Text(userId);
		context.write(outKey, new Text(fields.get(8) + "\t" + content));
	}

	public boolean isAvalidChar(char ch) {
		if(ch=='��'){
			return true;
		}
		if (0x4E00 > ch || ch > 0x9FA5) {
			return false;
		} else {
			return true;
		}
	}
	
	public static void main(String[] args) {
//		String str = "1508	2058228704	138897214:29221	150941975	�򰮺���	1		��ϲ�����ŵĸо�~	138897214		5	0				2009-05-20 11:08:30	2011-09-27 18:53:47		0		0";
		String str = "2010-08-25 ������13:41:42  ѩ�����ֱ���0957  ���ѽ���Ա��������ƭ������ʡǮ��������ܸ��ö�����һλ�������ֵĲ��ͣ����������";
		
		str = str.replaceAll(
						"(@[\\S&&[^\\p{Punct}]]+((\\({1}[\\S&&[^\\p{Punct}]]+\\,{1}\\d+\\){1})|(\\[{1}[\\S&&[^\\p{Punct}]]+\\,{1}\\d+\\]{1})))|^(((���){1}|(����){1}|(�ظ�){1})([\\S&&[^\\p{Punct}]&&[^\\p{Space}]&&[^\\uFF00-\\uFFEF]]){1,10})|([\\S&&[^\\p{Punct}]&&[^\\p{Space}]&&[^\\uFF00-\\uFFEF]]{1,10}((���){1}|(����){1}))|(#{1}[^#]*#{1})",
						"��");
		try {
			String date = "2012-05-17 20:34:52";
			SimpleDateFormat df = new SimpleDateFormat("yy-MM-dd HH:mm:ss"); 
			Date gmt_create = df.parse(date);
			long cur = new Date().getTime();
			long create = gmt_create.getTime();
			long delta =  cur -create; 
			long d = delta - HALFYEAR;
			if( (new Date().getTime() - gmt_create.getTime()) > HALFYEAR )
				System.out.println("sdsaf");
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		System.out.println(str);

		String str2 = "/group/taobao/taobao/dw/stb/yyyyMMdd/comment";
		Calendar cal = Calendar.getInstance();
		String datepath = "" +cal.get(Calendar.YEAR) + (cal.get(Calendar.MONTH)+1) + cal.get(Calendar.DAY_OF_MONTH);
		String path = str2.replaceAll("yyyyMMdd", datepath);
		System.out.println("path="+path);
		SimpleDateFormat s = new SimpleDateFormat("yyyyMMdd");
		cal.set(Calendar.DAY_OF_MONTH, cal.get(Calendar.DAY_OF_MONTH)-5);
		System.out.println("day of month:"+cal.get(Calendar.DAY_OF_MONTH));
		String strdate = s.format(cal.getTime());
		System.out.println("cal date = "+strdate);
	}
}
