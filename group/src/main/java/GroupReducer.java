import java.io.IOException;
import java.text.DateFormat;
import java.text.NumberFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.TreeMap;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class GroupReducer extends Reducer<Text, Text, Text, Text> {
	private static final long dayUnit = (long)24 * 3600 * 1000;
	private static final long now = new Date().getTime();
	private int TYPE_INDEX = 0;
	private String TYPE_T = "1";
	private String TYPE_B = "2";
	// index for s_group_group_thread_plus
	private int AUTHOR_ID_INDEX_T = 1;
	private int GMT_CREATE_INDEX_T = 2;
	// index for s_group_thread_body_plus
	private int CONTENT_INDEX_B  = 1;


	@Override
	public void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		Iterator<Text> it = values.iterator();
		GroupThread gt = new GroupThread();
		while (it.hasNext()) {
			Text value = it.next();
			SimpleStringTokenizer simpleTokenizer = new SimpleStringTokenizer(value.toString(), "\t");
			List<String> fields = simpleTokenizer.getAllElements();
			String type = fields.get(TYPE_INDEX); 
			if(type == null || type.isEmpty())
				return;
			
			if(TYPE_T.equals(type)){
				gt.setDayoffset(fields.get(GMT_CREATE_INDEX_T));
				gt.setUserId(fields.get(AUTHOR_ID_INDEX_T));
			}
			else{
				gt.setContent(fields.get(CONTENT_INDEX_B));
			}
			
//			if(TYPE_T.equals(type)){
//				Map<String, String> map = null;
//				if(resultMap.containsKey(key.toString())){
//					map = resultMap.get(key.toString());
//					map.put(TYPE_T, fields.get(AUTHOR_ID_INDEX_T)+ "\t" + fields.get(GMT_CREATE_INDEX_T));
//				}
//				else{
//					map = new HashMap<String, String>();
//					map.put(TYPE_T, fields.get(AUTHOR_ID_INDEX_T)+ "\t" + fields.get(GMT_CREATE_INDEX_T));
//					resultMap.put(key.toString(), map);
//				}
//			}
//			else if(TYPE_B.equals(type)){
//				Map<String, String> map = null;
//				if(resultMap.containsKey(key.toString())){
//					map = resultMap.get(key.toString());
//					map.put(TYPE_B, fields.get(CONTENT_INDEX_B));
//				}
//				else{
//					map = new HashMap<String, String>();
//					map.put(TYPE_B, fields.get(CONTENT_INDEX_B));
//					resultMap.put(key.toString(), map);
//				}
//			}
		}
		
		if(gt.getContent() != null && gt.getUserId() != null && gt.getDayoffset() != null){
			StringBuilder sb = new StringBuilder();
			sb.append(gt.getUserId()).append("\t");
			sb.append(gt.getDayoffset()).append("\t");
			sb.append(gt.getContent());
			context.write(key, new Text(sb.toString()));
		}
				
	}
	
}
