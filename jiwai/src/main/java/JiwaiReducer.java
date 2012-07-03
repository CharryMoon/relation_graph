import java.io.IOException;
import java.text.NumberFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.StringTokenizer;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class JiwaiReducer extends Reducer<Text, Text, Text, Text> {

	@Override
	public void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		int commentCount = 0;
		int distinctCommentCount = 0;
		Map<String, Integer> contents = new HashMap<String, Integer>();
		Iterator<Text> it = values.iterator();
		while (it.hasNext()) {
			Text value = it.next();
			String content = value.toString();
			Integer count = contents.get(content);
			if (count == null) {
				count = new Integer(1);
				distinctCommentCount++;
			} else {
				count++;
			}
			contents.put(content, count);
			commentCount++;
		}
		NumberFormat format = NumberFormat.getInstance();
		format.setGroupingUsed(false);
		format.setMaximumFractionDigits(2);
		if (commentCount >= 10 && getRepeatRate(commentCount, distinctCommentCount) > 80 ) {
			StringBuilder sb = new StringBuilder();
			// sb.append(key.toString()).append("\t");
			sb.append(commentCount).append("\t");
			sb.append(distinctCommentCount).append("\t");
			sb.append(format.format((float)commentCount/distinctCommentCount)).append("\t");
			sb.append(getContents(contents));
			context.write(key, new Text(sb.toString()));
		}
	}

	private String getContents(Map<String, Integer> contents) {
		List<Map.Entry<String, Integer>> entries = Collections.list(Collections
				.enumeration(contents.entrySet()));
		Collections.sort(entries, new Comparator<Map.Entry<String, Integer>>() {

			public int compare(Map.Entry<String, Integer> entry1,
					Map.Entry<String, Integer> entry2) {
				if (entry1.getValue() == entry2.getValue()) {
					return 0;
				} else if (entry1.getValue() > entry2.getValue()) {
					return -1;
				} else {
					return 1;
				}
			}

		});
		StringBuilder sb = new StringBuilder();
		int i = 0;
		for (Map.Entry<String, Integer> entry : entries) {
			if (i > 4 && sb.length() > 1024) {
				break;
			}
			sb.append(entry.getKey()).append("(").append(entry.getValue())
					.append(")").append("|");
			i++;
		}
		if (sb.length() > 0) {
			return sb.substring(0, sb.length() - 1);
		} else {
			return "";
		}
	}
	
	private int getRepeatRate(int commentCount, int distinctCommentCount){
		float rate = (float)(commentCount-distinctCommentCount)/commentCount;
		return Math.round(rate*100);
	}
	
	public static void main(String[] args) {
		JiwaiReducer cr = new JiwaiReducer();
		System.out.println(cr.getRepeatRate(55, 12));
	}
}
