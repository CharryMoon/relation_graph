import java.io.IOException;
import java.io.RandomAccessFile;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;


public class FollowAvailabilityMapper extends
		Mapper<LongWritable, Text, FollowAvailabilityJoinKey, Text> {
	private Set<String> suspicionList = new HashSet<String>();
	private String source;
	private static final long dayUnit = 24 * 3600 * 1000;
	private static final long peroid = 180 * dayUnit;
	private long now = 0;

	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		if (source != "f".intern() && source != "s".intern()) {
			return;
		}
		String line = value.toString();
		List<String> fields = null;
		if (source == "f".intern()) {
			SimpleStringTokenizer tokenizer = new SimpleStringTokenizer(line,
					"\01");
			fields = tokenizer.getAllElements();
		} else {
			SimpleStringTokenizer tokenizer = new SimpleStringTokenizer(line,
					"\t");
			fields = tokenizer.getAllElements();
		}
		String userId = null;
		userId = fields.get(1);
		String subscribeId = fields.get(2);
		if (!suspicionList.contains(userId)) {
			if (source == "f".intern()) {
				context.getCounter("ingoreData", "suspicionList-follow")
						.increment(1);
			} else {
				context.getCounter("ingoreData",
						"suspicionList-follow-statistic").increment(1);
			}
			return;
		}
		DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		Date gmtCreate;
		try {
			gmtCreate = dateFormat.parse(fields.get(5));
		} catch (ParseException e) {
			throw new RuntimeException(fields.get(5), e);
		}
		long day = gmtCreate.getTime();
		if (day < (now - peroid) || day > now) {
			if (source == "f".intern()) {
				context.getCounter("ingoreData", "expireData-follow")
						.increment(1);
			} else {
				context.getCounter("ingoreData", "expireData-follow-statistic")
						.increment(1);
			}
			return;
		}
		context.write(new FollowAvailabilityJoinKey(userId, subscribeId),
				new Text(source));
	}

	@Override
	protected void cleanup(Context context) throws IOException,
			InterruptedException {
		suspicionList.clear();
		super.cleanup(context);
	}

	@Override
	protected void setup(Context context) throws IOException,
			InterruptedException {
		super.setup(context);
		Configuration conf = context.getConfiguration();
		Path[] paths = DistributedCache.getLocalCacheFiles(conf);
		for (Path path : paths) {
			loadSuspicionList(path.toUri().getPath());
		}
		FileSplit split = (FileSplit) context.getInputSplit();
		String path = split.getPath().toUri().getPath();
		if (path.indexOf("sns_follow_statistic") > 0) {
			source = "s".intern();
		} else if (path.indexOf("sns_subscribe") > 0) {
			source = "f".intern();
		} else {
			throw new RuntimeException("unknow data source." + path);
		}
		now = context.getConfiguration().getLong("follow.current.day",
				System.currentTimeMillis());
	}

	private void loadSuspicionList(String fileName) throws IOException {
		RandomAccessFile file = new RandomAccessFile(fileName, "r");
		try {
			String line = null;
			while ((line = file.readLine()) != null) {
				line = new String(line.getBytes("ISO-8859-1"), "UTF-8");
				int pos = line.indexOf('\t');
				suspicionList.add(line.substring(0, pos));
			}
		} finally {
			file.close();
		}
	}

	public static final class CombinerReduce
			extends
			Reducer<FollowAvailabilityJoinKey, Text, FollowAvailabilityJoinKey, Text> {
		protected void reduce(FollowAvailabilityJoinKey key,
				Iterable<Text> values, Context context) throws IOException,
				InterruptedException {
			Iterator<Text> it = values.iterator();
			Set<String> sourceSet = new HashSet<String>();
			while (it.hasNext()) {
				String source = it.next().toString();
				sourceSet.add(source);
			}
			for (String source : sourceSet) {
				context.write(key, new Text(source));
			}

		}
	}
}
