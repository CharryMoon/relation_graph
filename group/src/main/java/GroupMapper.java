import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class GroupMapper extends Mapper<LongWritable, Text, Text, Text> {
	private static final long HALFYEAR = (long)60*60*24*1000*90;
	private static final long dayUnit = 24 * 3600 * 1000;
	private static final long now = new Date().getTime();
	// index for s_group_group_thread_plus
	private int THREAD_ID_INDEX_T = 0;
	private int DELETED_INDEX_T = 8;
	private int GMT_CREATE_INDEX_T = 19;
	// index for s_group_thread_body_plus
	private int THREAD_ID_INDEX_B  = 0;
	private int CONTENT_INDEX_B  = 2;
	
	private String source = "";
	
	public void map(LongWritable key, Text values, Context context)
			throws IOException, InterruptedException {
		if (source != "t".intern() && source != "b".intern()) {
			return;
		}

		String line = values.toString().toLowerCase();
		List<String> fields = null;
		SimpleStringTokenizer simpleTokenizer = new SimpleStringTokenizer(line, "\t");
		fields = simpleTokenizer.getAllElements();
		if(source == "t".intern()){
			try {
				//取最近半年的
				SimpleDateFormat df = new SimpleDateFormat("yy-MM-dd HH:mm:ss"); 
				Date gmt_create = df.parse(fields.get(GMT_CREATE_INDEX_T));
				if(now - gmt_create.getTime() > HALFYEAR ){
					return;
				}
				// 去掉删除的
				if("1".equals(fields.get(DELETED_INDEX_T)))
					return;
				
				Date d = null;
				SimpleDateFormat sdf = new SimpleDateFormat("yy-MM-dd");
				d = sdf.parse(fields.get(GMT_CREATE_INDEX_T).substring(0,10));
				context.write(new Text(fields.get(THREAD_ID_INDEX_T)), new Text((now-d.getTime())/dayUnit + "\t" + "1"));
			} catch (Exception e) {
				return;
			}
		}
		else{
			context.write(new Text(fields.get(THREAD_ID_INDEX_B)), new Text(fields.get(CONTENT_INDEX_B) +"\t"+ "2"));
		}
	}
	
	protected void setup(Mapper<LongWritable,Text,Text,Text>.Context context) 
			throws IOException ,InterruptedException {
		super.setup(context);
		FileSplit split = (FileSplit) context.getInputSplit();
		String path = split.getPath().toUri().getPath();
		if (path.indexOf("group_thread_body_plus") > 0) {
			source = "t".intern();
		} else if (path.indexOf("group_group_thread_plus") > 0) {
			source = "b".intern();
		} else {
			throw new RuntimeException("unknow data source." + path);
		}
	};
	
	public static void main(String[] args) {
//		String str = "<p>分享图片<img alt=\"\" src=\"http://img04.taobaocdn.com/sns_album/i4/T1JI1DXl4MXXb1upjX.jpg\" /></p>";
		String str = "<p>分享图片<div class=\"pic-detail\"><div class=\"pic-detail-pic\"><img class=\"\"  src=\"http://img01.taobaocdn.com/imgextra/i1/15728017142114453/T1HlToXo4kXXXXXXXX_!!114585728-0-grouplus.jpg_570x10000.jpg\" /><div class=\"pic-detail-tools\"><a href=\"http://img01.taobaocdn.com/imgextra/i1/15728017142114453/T1HlToXo4kXXXXXXXX_!!114585728-0-grouplus.jpg\" target=\"_blank\" class=\"pic-detail-big\"></a></div></div></div></p>";

		String stripStr = str.replaceAll("\\<.*?>","");
		System.out.println(stripStr);
	}
}
