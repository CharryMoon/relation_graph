import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class FilePathPartitionUtils {

	public static final String PARTITION_TIME = "sns.relation.file.partition.time";

	public static Map<String, String> getLastPartitionPath(Configuration conf,
			final String input, final String output) throws IOException {
		SimpleDateFormat format = new SimpleDateFormat("yyyyMMdd");
		Calendar current = Calendar.getInstance();
		String strDate = format.format(current.getTime());
		FileSystem fs = FileSystem.get(conf);
		String in = input;
		String out = output;
		int i = 0;
		boolean replaced = false;
		for (i = 0; i < 10; i++) {
			in = input.replace("*", strDate);
			if (fs.exists(new Path(in))) {
				out = output.replace("*", strDate);
				replaced = true;
				break;
			} else {
				current.add(Calendar.DAY_OF_MONTH, -1);
				strDate = format.format(current.getTime());
			}
		}

		Map<String, String> result = new HashMap<String, String>();
		result.put("input", in);
		result.put("output", out);
		String day;
		if (replaced) {
			day = strDate;
		} else {
			day = format.format(new Date());
		}

		try {
			Date date = format.parse(day);
			conf.setLong(PARTITION_TIME, date.getTime() + (24 * 3600 * 1000)
					- 1);
		} catch (ParseException e) {
			throw new RuntimeException(e);
		}

		return result;
	}
}
