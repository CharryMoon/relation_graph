package sameip;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import sameschool.Student;
import util.IPutil;
import util.SimpleStringTokenizer;

/**
 * 同ip的计算源数据来源于,之前已经计算过一次的ip数据.
 * 从用户的所有的的至多1024个ip中取出出现次数最多的3个来进行推测计算.
 * 我们使用这3个ip来进行计算
 * @author leibiao
 *
 */
public class SameIP extends Configured implements Tool {
	private static final int MAX_RECOMMEND = 800;
	
	public static class SameIPStep1Mapper extends Mapper<LongWritable, Text, Text, Text> {
		private final int USER_ID_INDEX = 0;
		private final int IP_INDEX = 1;
		private final int USER_IP_MAX = 2;

		protected void map(LongWritable key, Text value, Context context) 
				throws IOException ,InterruptedException {
			List<String> fields = new SimpleStringTokenizer(value.toString(), "\t").getAllElements();
			if(fields.size() < 2)
				return;
			
			String[] ips = fields.get(IP_INDEX).split(",");
			
			int limit = USER_IP_MAX > ips.length? ips.length:USER_IP_MAX;
			for (int i = 0; i < limit; i++) {
				int ip = IPutil.ipToInt(ips[i]);
				if(ip == 0)
					continue;
				if(StringUtils.isBlank(fields.get(USER_ID_INDEX)))
					continue;
				context.write(new Text(String.valueOf(ip)), new Text(fields.get(USER_ID_INDEX)));
			}
		}
	}
	
	public static final class CombinerReduce extends
			Reducer<Text, Text, Text, Text> {
		private final int ONEMAP_SAMEIP_MAX = 5000;
		protected void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			Iterator<Text> it = values.iterator();
			StringBuilder sb = new StringBuilder();
			int count = 0;
			while (it.hasNext()) {
				if(count++ > ONEMAP_SAMEIP_MAX)
					break;
				
				if(sb.length() > 0)
					sb.append(",");
				
				sb.append(it.next().toString());
			}
			if(sb.length() > 0)
				context.write(key, new Text(sb.toString()));
		}
	}
	
	public static class SameIPStep1Reducer extends Reducer<Text, Text, Text, Text> {
		private final static int MAX_USERS = 10000;
		
		protected void reduce(Text key, Iterable<Text> values, Context context) 
				throws IOException ,InterruptedException {
			List<String> users = new ArrayList<String>();
			String fromUid = null;
			String toUid = null;
			Iterator<Text> itor = values.iterator();
			System.out.println(new Date());
			while (itor.hasNext()) {
//				String userIds[] = itor.next().toString().split(",");
//				for(String userId : userIds){
//					if(StringUtils.isBlank(userId))
//						continue;
//					if(!users.contains(userId))
//						users.add(userId);
//				}
				String userId = itor.next().toString();
				if (StringUtils.isBlank(userId))
					continue;
				if (!users.contains(userId))
					users.add(userId);
				if (users.size() > MAX_USERS) {
					context.setStatus("size to big");
					return ;
				}
			}
			System.out.println(new Date());
			
			for (int i = 0,count=0; i < users.size()&&count<5000; i++,count++) {
				context.progress();
				fromUid = users.get(i);
				context.setStatus("fromUid:"+fromUid);
				for (int j = 0,limit = 0; j < users.size() && limit < 800; j++,limit++) {
					toUid = users.get(j);
					if (fromUid.equals(toUid)) {
						continue;
					}
					context.write(new Text(fromUid), new Text(toUid +"\t"+ key.toString()));
				}
			}
		}
	}
	
	public int run(String[] args) throws Exception {
		Job job = new Job(getConf());
		job.setJarByClass(SameIP.class);
		job.setJobName("Same ip analyze");
		job.setNumReduceTasks(100);
		job.getConfiguration().set("mapred.child.java.opts","-Xmx2048m");

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		job.setMapperClass(SameIP.SameIPStep1Mapper.class);
		job.setReducerClass(SameIP.SameIPStep1Reducer.class);
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
		int ret = ToolRunner.run(new SameIP(), args);
		System.exit(ret);
	}
	
}
