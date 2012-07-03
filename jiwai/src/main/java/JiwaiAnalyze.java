import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.*;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class JiwaiAnalyze extends Configured implements Tool  {

	public int run(String[] args) throws Exception {
		Job job = new Job(getConf());
		job.setJarByClass(JiwaiAnalyze.class);
		job.setJobName("JiwaiAnalyze");
		job.setNumReduceTasks(5);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		job.setMapperClass(JiwaiMapper.class);
		job.setReducerClass(JiwaiReducer.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		// 由于zeus的时间参数不能正常运作,所以这里我们自己指定时间.
		// 替换 参数中的yyyyMMdd
		Calendar cal = Calendar.getInstance();
		cal.set(Calendar.DAY_OF_MONTH, cal.get(Calendar.DAY_OF_MONTH)-1);
		SimpleDateFormat s = new SimpleDateFormat("yyyyMMdd");
		String datepath = s.format(cal.getTime());
		String path = args[0].replaceAll("yyyyMMdd", datepath);
		FileInputFormat.setInputPaths(job, new Path(path));
		String outpath = args[1].replaceAll("yyyyMMdd", datepath);
		FileOutputFormat.setOutputPath(job, new Path(outpath));

		boolean success = job.waitForCompletion(true);
		return success ? 0 : 1;
	}

	public static void main(String[] args) throws Exception {
		int ret = ToolRunner.run(new JiwaiAnalyze(), args);
		System.exit(ret);
	}
	
}
