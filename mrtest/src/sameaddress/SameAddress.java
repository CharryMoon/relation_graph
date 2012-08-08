package sameaddress;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import util.SimpleStringTokenizer;


public class SameAddress extends Configured implements Tool {
	private final static int USER_ID = 0;
	private final static int PHONE = 1;
	private final static int MAX_USERS = 100;
	private final static int PHONE_LENGTH = 11;
	
	public static class MapClass extends Mapper<LongWritable, Text, Text, Text> {
		
		@Override
		public void map(LongWritable key, Text value, Context context) 
				throws IOException ,InterruptedException {
			List<String> fields = new SimpleStringTokenizer(value.toString(), "\t").getAllElements();
			String[] phones = fields.get(PHONE).split(",");
			for (int i = 0; i < phones.length; i++) {
				if (!isValidCellphone(phones[i])) {
					continue;
				}
				context.write(new Text(phones[i]),
						new Text(fields.get(USER_ID)));
			}
			
		}

		private boolean isValidCellphone(String string) {
			char ch;
			int firstThree = 0;
			
			if (string.length() != PHONE_LENGTH) {
				return false;
			}
			
			for (int i = string.length() - 1; i >= 0; i--) {
				ch = string.charAt(i);
				if (ch < '0' || ch > '9') {
					return false;
				}
			}
			
			firstThree = Integer.valueOf(string.substring(0, 3));
			
			if (firstThree == 154 || firstThree == 184 || firstThree < 130
					|| (159 < firstThree && firstThree < 180) || firstThree > 189) {
				return false;
			}
			
			return true;
		}
		
	}
	
	public static class Reduce extends Reducer<Text, Text, Text, Text> {

		@Override
		public void reduce(Text key, Iterable<Text> values, Context context) 
				throws IOException ,InterruptedException {
			Set<String> sets = new HashSet<String>();
			List<String> users = new ArrayList<String>();
			String userAId = null;
			String userBId = null;
			int usersSize = -1;
			Iterator<Text> itor = values.iterator();
			while (itor.hasNext()) {
				sets.add(itor.next().toString());
			}
			usersSize = sets.size();
			if (usersSize > MAX_USERS) {
				return ;
			}
			users.addAll(sets);
			
			for (int i = 0; i < usersSize; i++) {
				userAId = users.get(i);
				for (int j = 0; j < usersSize; j++) {
					userBId = users.get(j);					
					if (userAId.equals(userBId)) {
						continue;
					}					
					context.write(key, new Text(userAId + "\t" 
										+ userBId));
				}
			}
		}
	}
	
	@Override
	public int run(String[] args) throws Exception {
		Job job = new Job(getConf());
		job.setJarByClass(SameAddress.class);
		job.setJobName("Same cellphone user...");
		job.setNumReduceTasks(100);
		job.getConfiguration().set("mapred.child.java.opts","-Xmx512m");
		job.getConfiguration().set("mapred.job.queue.name", "cug-taobao-sns");

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		job.setMapperClass(MapClass.class);
		job.setReducerClass(Reduce.class);
//		job.setCombinerClass(SameIP.CombinerReduce.class);

		job.setInputFormatClass(TextInputFormat.class);
		SequenceFileOutputFormat.setOutputCompressionType(job, SequenceFile.CompressionType.BLOCK);
		job.setOutputFormatClass(SequenceFileOutputFormat.class);

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
		int status = ToolRunner.run(new Configuration(), new SameAddress(), args);
		System.exit(status);
	}

}
