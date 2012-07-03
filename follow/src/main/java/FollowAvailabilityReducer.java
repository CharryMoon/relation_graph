import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Reducer;

public class FollowAvailabilityReducer extends
		Reducer<FollowAvailabilityJoinKey, Text, Text, Text> {

	private Map<String, String> suspicionMap = new HashMap<String, String>();
	private String previewUserId;
	private int avalidCount = 0;
	private boolean firstTime = true;

	@Override
	protected void setup(Context context) throws IOException,
			InterruptedException {
		super.setup(context);
		Configuration conf = context.getConfiguration();
		Path[] paths = DistributedCache.getLocalCacheFiles(conf);
		for (Path path : paths) {
			loadSuspicionList(path.toUri().getPath());
		}
	}

	private void loadSuspicionList(String fileName) throws IOException {
		RandomAccessFile file = new RandomAccessFile(fileName, "r");
		try {
			String line = null;
			while ((line = file.readLine()) != null) {
				line = new String(line.getBytes("ISO-8859-1"),"UTF-8");
				int pos = line.indexOf('\t');
				suspicionMap.put(line.substring(0, pos),
						line.substring(pos + 1));
			}
		} finally {
			file.close();
		}
	}

	@Override
	protected void reduce(FollowAvailabilityJoinKey key, Iterable<Text> values,
			Context context) throws IOException, InterruptedException {
		Iterator<Text> it = values.iterator();
		String userId = key.getUserId().toString();
		if (firstTime) {
			avalidCount = 0;
			previewUserId = userId;
			firstTime = false;
		} else if (!previewUserId.equals(userId)) {
			String v = suspicionMap.get(previewUserId);
			StringBuilder sb = new StringBuilder(v);
			sb.insert(sb.indexOf("\t"), "\t" + avalidCount);
			context.write(new Text(previewUserId), new Text(sb.toString()));
			avalidCount = 0;
			previewUserId = userId;
		}
		Set<String> sourceSet = new HashSet<String>();
		while (it.hasNext()) {
			String source = it.next().toString();
			sourceSet.add(source);
		}
		if (sourceSet.size() == 2) {
			avalidCount++;
		}
	}

	@Override
	protected void cleanup(Context context) throws IOException,
			InterruptedException {
		if (previewUserId != null) {
			String v = suspicionMap.get(previewUserId);
			StringBuilder sb = new StringBuilder(v);
			sb.insert(sb.indexOf("\t"), "\t" + avalidCount);
			context.write(new Text(previewUserId), new Text(sb.toString()));
		}
		super.cleanup(context);
	}

	public static class GroupComparator extends WritableComparator {

		protected GroupComparator(Class<FollowAvailabilityJoinKey> keyClass) {
			super(keyClass, true);
		}

		@SuppressWarnings("rawtypes")
		@Override
		public int compare(WritableComparable a, WritableComparable b) {
			return ((FollowAvailabilityJoinKey) a).getUserId().compareTo(
					((FollowAvailabilityJoinKey) b).getUserId());
		}
	}

	public static void main(String[] args) {
		StringBuilder sb = new StringBuilder("1	2	3");
		sb.insert(sb.indexOf("\t"), "\t" + "avalidCount");
		System.out.println(sb.toString());
	}
}
