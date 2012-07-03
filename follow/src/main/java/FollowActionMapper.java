import java.io.IOException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.StringTokenizer;
import java.util.TreeMap;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;


public class FollowActionMapper extends Mapper<LongWritable, Text, Text, Text> {
	private static final long dayUnit = 24 * 3600 * 1000;
	private static final long peroid = 180 * dayUnit;
	private long now = 0;

	@Override
	protected void setup(Context context) throws IOException,
			InterruptedException {
		super.setup(context);
		now = context.getConfiguration().getLong(
				FilePathPartitionUtils.PARTITION_TIME,
				System.currentTimeMillis());
	}

	public void map(LongWritable key, Text values, Context context)
			throws IOException, InterruptedException {
		DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		String line = values.toString();
		List<String> fields = new ArrayList<String>();
		StringTokenizer itr = new StringTokenizer(line, "\t");
		while (itr.hasMoreElements()) {
			fields.add(itr.nextToken());
		}
		String userId = fields.get(1);
		String gmtCreate = fields.get(5);
		Date date;
		long day = 0;
		try {
			date = dateFormat.parse(gmtCreate);
			day = date.getTime();
		} catch (ParseException e) {
			throw new RuntimeException(gmtCreate, e);
		}
		if (now - peroid > day || day > now) {
			context.getCounter("ingoreData", "expireData").increment(1);
			return;
		}
		Text outKey = new Text(userId);
		context.write(outKey, new Text((now - day) / dayUnit + "\t" + "1"));
	}

	public static final class CombinerReduce extends
			Reducer<Text, Text, Text, Text> {
		protected void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			Iterator<Text> it = values.iterator();
			Map<Integer, Integer> sumMap = new TreeMap<Integer, Integer>();
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
			}
			for (Integer day : sumMap.keySet()) {
				context.write(key, new Text(day + "\t" + sumMap.get(day)));
			}

		}
	}

}
