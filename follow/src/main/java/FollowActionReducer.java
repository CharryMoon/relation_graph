import java.io.IOException;
import java.text.DateFormat;
import java.text.NumberFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


public class FollowActionReducer extends Reducer<Text, Text, Text, Text> {

	private static final long dayUnit = 24 * 3600 * 1000;
	private long now = 0;

	@Override
	protected void setup(Context context) throws IOException,
			InterruptedException {
		super.setup(context);
		now = context.getConfiguration().getLong(FilePathPartitionUtils.PARTITION_TIME,
				System.currentTimeMillis());
	}

	@Override
	protected void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		Iterator<Text> it = values.iterator();
		Map<Integer, Integer> sumMap = new TreeMap<Integer, Integer>();
		int total = 0;
		// 计算每天关注数量
		while (it.hasNext()) {
			Text value = it.next();
			String line = value.toString();
			int pos = line.indexOf('\t');
			String date = line.substring(0, pos);
			int day = Integer.valueOf(date);
			int count = Integer.valueOf(line.substring(pos + 1));
			Integer sum = sumMap.get(day);
			if (sum == null) {
				sum = new Integer(0);
			}
			sum += count;
			sumMap.put(day, sum);
			total += count;
		}
		List<Map<Integer, Integer>> continuableList = new ArrayList<Map<Integer, Integer>>();
		int maxSum = -1;
		int maxDay = -1;
		int prevDay = -1;
		// 对关注按照时间连续性进行分组。
		Map<Integer, Integer> continuableMap = null;
		for (Integer day : sumMap.keySet()) {
			int sum = sumMap.get(day);
			if (sum > maxSum) {
				maxSum = sum;
				maxDay = day;
			}
			// 连续的或隔一天
			if (prevDay > 0
					&& (day.equals(prevDay + 1) || day.equals(prevDay + 2))) {
				continuableMap.put(day, sum);
			} else {
				if (continuableMap != null) {
					continuableList.add(continuableMap);
				}
				continuableMap = new TreeMap<Integer, Integer>();
				continuableMap.put(day, sum);
			}
			prevDay = day;
		}

		String msg = buildRuleMessage(maxDay, maxSum, continuableList);
		if (msg == null) {
			return;
		}
		StringBuilder sb = new StringBuilder();
		sb.append(total).append("\t");
		sb.append(maxSum).append("\t");
		sb.append(sumMap.size()).append("\t");
		sb.append(msg).append("(");
		sb.append(buildValue(sumMap)).append(")");
		context.write(key, new Text(sb.toString()));
	}

	private Map<String, Integer> checkRule(Map<Integer, Integer> sumMap) {
		int size = sumMap.size();
		int critical = getCritical(size);

		int minFollow = Integer.MAX_VALUE;
		int minDay = -1;
		int maxFollow = -1;
		int maxDay = -1;
		int total = 0;
		int firstDay = 0;
		int lastDay = 0;
		int i = 0;
		for (Integer day : sumMap.keySet()) {
			if (i == 0) {
				firstDay = day;
			}
			if (i == size - 1) {
				lastDay = day;
			}
			i++;
			Integer sum = sumMap.get(day);
			total += sum;
			if (minFollow > sum) {
				minFollow = sum;
				minDay = day;
			}
			if (maxFollow < sum) {
				maxFollow = sum;
				maxDay = day;
			}
		}
		float average = (float) total / size;
		if (average >= critical) {
			Map<String, Integer> context = new HashMap<String, Integer>();
			context.put("minDay", minDay);
			context.put("minFollow", minFollow);
			context.put("maxDay", maxDay);
			context.put("maxFollow", maxFollow);
			context.put("totalDays", size);
			context.put("totalFollow", total);
			context.put("critical", critical);
			context.put("firstDay", firstDay);
			context.put("lastDay", lastDay);
			return context;
		} else {
			return null;
		}
	}

	private String buildRuleMessage(int maxDay, int maxSum,
			List<Map<Integer, Integer>> continuableList) {
		DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
		StringBuilder sb = new StringBuilder();
		if (maxSum >= 100) {
			sb.append("加关注最多的一天")
					.append(dateFormat.format(new Date(now - maxDay * dayUnit)))
					.append("关注了").append(maxSum).append("次。");
		}
		for (Map<Integer, Integer> continuable : continuableList) {
			if (continuable != null && continuable.size() > 2) {
				Map<String, Integer> context = checkRule(continuable);
				if (context != null) {
					sb.append(buildRuleMessage(context));
				}
			}
		}
		if (sb.length() > 0) {
			return sb.toString();
		} else {
			return null;
		}
	}

	private int getCritical(int size) {
		int critical = 1;
		if (size == 3) {
			critical = 49;
		} else if (size > 3 && size <= 5) {
			critical = 29;
		} else if (size > 5 && size <= 10) {
			critical = 19;
		} else if (size > 10 && size <= 30) {
			critical = 9;
		} else if (size > 30 && size <= 60) {
			critical = 4;
		} else if (size > 60) {
			critical = 1;
		} else {
			critical = Integer.MAX_VALUE;
		}
		return critical;
	}

	private String buildRuleMessage(Map<String, Integer> context) {
		DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
		NumberFormat format = NumberFormat.getInstance();
		format.setGroupingUsed(false);
		format.setMaximumFractionDigits(2);

		StringBuilder sb = new StringBuilder();

		int minFollow = context.get("minFollow");
		int minDay = context.get("minDay");
		int maxFollow = context.get("maxFollow");
		int maxDay = context.get("maxDay");
		int totalFollow = context.get("totalFollow");
		int totalDays = context.get("totalDays");
		int critical = context.get("critical");
		int firstDay = context.get("firstDay");
		int lastDay = context.get("lastDay");
		float average = (float) totalFollow / totalDays;

		sb.append("连续").append(totalDays).append("[")
				.append(dateFormat.format(new Date(now - lastDay * dayUnit)))
				.append(",")
				.append(dateFormat.format(new Date(now - firstDay * dayUnit)))
				.append("]天加关注，");
		sb.append("平均每天关注").append(format.format(average)).append("次,");
		sb.append("大于临界值").append(critical).append("次，");
		sb.append("连续区间内最多的一天")
				.append(dateFormat.format(new Date(now - maxDay * dayUnit)))
				.append("加关注").append(maxFollow).append("次，");
		sb.append("最少的一天")
				.append(dateFormat.format(new Date(now - minDay * dayUnit)))
				.append("加关注").append(minFollow).append("次。");
		return sb.toString();
	}

	private String buildValue(Map<Integer, Integer> sumMap) {
		StringBuilder sb = new StringBuilder();
		for (Integer day : sumMap.keySet()) {
			sb.append(day).append(":").append(sumMap.get(day)).append(",");
		}
		sb.deleteCharAt(sb.length() - 1);
		return sb.toString();
	}

}
