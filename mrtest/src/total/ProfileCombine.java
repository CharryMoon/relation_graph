package total;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import org.apache.commons.lang.math.NumberUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.lib.MultipleInputs;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.eclipse.jdt.internal.core.util.Util.Comparer;

import follow.CommonFollow;

import util.SimpleStringTokenizer;

/**
 * 合并互动数据和个人属性数据. 互动数据已经是合并过一次的了.
 * 将所有同一个人的数据合并为 userId flag value
 * value: targetId score, targetId score, targetId score
 * 分隔符用定义好的 \002,\003作为一级和二级分隔符
 * @author leibiao
 *
 */

public class ProfileCombine extends Configured implements Tool {
	// 默认分隔符
	private final static String FIELD_SEPERATOR = "\001";
	// from
	private static int FROM_INTERACTIVE = 1;
	private static int FROM_PROFILE = 2;
	private static int FROM_SHOPPING = 3;
	private static int fromtype = 0;
	// 大维度的总数
	private static double degree_count = 3.0;
	
	public static class InteractiveMapper extends MapReduceBase implements
			Mapper<Text, Text, Text, Text> {
		private final static int FROM_USER_ID = 0;
		private final static int TO_USER_ID = 1;
		private final static int TYPE_INDEX = 2;
		private final static int SCORE_INDEX = 3;
		
		@Override
		public void map(Text key, Text value,
				OutputCollector<Text, Text> output, Reporter reporter)
				throws IOException {
			List<String> fields = new SimpleStringTokenizer(value.toString(), FIELD_SEPERATOR).getAllElements();
			if(NumberUtils.toLong(fields.get(FROM_USER_ID), 0) == 0)
				return;
			if(NumberUtils.toLong(fields.get(TO_USER_ID), 0) == 0)
				return;
			
			double score = NumberUtils.toDouble(fields.get(SCORE_INDEX),0)/degree_count;
			output.collect(new Text(fields.get(FROM_USER_ID)),
					new Text(fields.get(TO_USER_ID)+FIELD_SEPERATOR
							+fields.get(TYPE_INDEX)+FIELD_SEPERATOR
							+score+FIELD_SEPERATOR
							+FROM_INTERACTIVE));
		}
	}

	public static class ProfileMapper extends MapReduceBase implements
			Mapper<Text, Text, Text, Text> {
		private final static int FROM_USER_ID = 0;
		private final static int TYPE_INDEX = 1;
		private final static int TO_USER_ID = 2;
		private final static int COUNT_INDEX = 3;
		private final static int SCORE_INDEX = 4;
		private final static int VALUE_INDEX = 5;
		// 总数,目前写死
		private static double total = 6.0;

		@Override
		public void map(Text key, Text value,
				OutputCollector<Text, Text> output, Reporter reporter)
				throws IOException {
			List<String> fields = new SimpleStringTokenizer(value.toString(),
					FIELD_SEPERATOR).getAllElements();
			if (NumberUtils.toLong(fields.get(FROM_USER_ID), 0) == 0)
				return;
			if (NumberUtils.toLong(fields.get(TO_USER_ID), 0) == 0)
				return;

			double score = NumberUtils.toDouble(fields.get(SCORE_INDEX), 0);
			score *= 1 / total;
			score /= degree_count;

			output.collect(
					new Text(fields.get(FROM_USER_ID)),
					new Text(fields.get(TO_USER_ID)+FIELD_SEPERATOR 
							+ fields.get(TYPE_INDEX) + FIELD_SEPERATOR 
							+ score+ FIELD_SEPERATOR 
							+ FROM_PROFILE));
		}
		
		@Override
		public void configure(JobConf job) {
			total = NumberUtils.toDouble(job.get("profileDataCount"), 0);
			if(total==0)
				System.out.println("get total count err");
			super.configure(job);
		}
	}

	public static class ShopMapper extends MapReduceBase implements
			Mapper<LongWritable, Text, Text, Text> {
		private final static int FROM_USER_ID = 0;
		private final static int TO_USER_ID = 1;
		private final static int TYPE_INDEX = 2;
		private final static int SCORE_INDEX = 3;
		//
		private final static int shopType = 25;

		@Override
		public void map(LongWritable key, Text value,
				OutputCollector<Text, Text> output, Reporter reporter)
				throws IOException {
			List<String> fields = new SimpleStringTokenizer(value.toString(),"\t").getAllElements();
			if(fields.size() < 4)
				return;
			
			if (NumberUtils.toLong(fields.get(FROM_USER_ID), 0) == 0)
				return;
			if (NumberUtils.toLong(fields.get(TO_USER_ID), 0) == 0)
				return;

			double score = NumberUtils.toDouble(fields.get(SCORE_INDEX), 0);
			score = 100000 * score /(degree_count*3);

			output.collect(new Text(fields.get(FROM_USER_ID)),
					new Text(fields.get(TO_USER_ID)+FIELD_SEPERATOR 
							+ shopType + FIELD_SEPERATOR 
							+ score+ FIELD_SEPERATOR 
							+ FROM_SHOPPING));
		}
	}	

	public static class ProfileCombineReducer extends MapReduceBase implements
			Reducer<Text, Text, Text, Text> {
		private final static int TO_USERID_INDEX = 0;
		private final static int TYPE_INDEX = 1;
		private final static int SCORE = 2;
		private final static int LOCALTYPE_INDEX = 3;

		@Override
		public void reduce(Text key, Iterator<Text> values,
				OutputCollector<Text, Text> output, Reporter reporter)
				throws IOException {
			List<SaveDataBlock> targets = new ArrayList<ProfileCombine.ProfileCombineReducer.SaveDataBlock>();
			Map<String, SaveDataBlock> allUsers = new HashMap<String, ProfileCombine.ProfileCombineReducer.SaveDataBlock>();
			while (values.hasNext()) {
				List<String> fields = new SimpleStringTokenizer(values.next().toString(), FIELD_SEPERATOR).getAllElements();
				if(allUsers.containsKey(fields.get(TO_USERID_INDEX))){
					int type = NumberUtils.toInt(fields.get(LOCALTYPE_INDEX), 0);
					if(type == 0)
						continue;
					int localtype = NumberUtils.toInt(fields.get(LOCALTYPE_INDEX), 0);
					allUsers.get(fields.get(TO_USERID_INDEX)).addData(fields.get(TO_USERID_INDEX), fields.get(SCORE), fields.get(TYPE_INDEX), localtype);
				}
				else{
					SaveDataBlock s = new SaveDataBlock(fields.get(TO_USERID_INDEX), fields.get(SCORE), fields.get(TYPE_INDEX));
					allUsers.put(fields.get(TO_USERID_INDEX), s);
				}
			}
			
			for(SaveDataBlock s : allUsers.values()){
				if(s.score == 0)
					continue;
				targets.add(s);
			}
			
			Collections.sort(targets, new SimpleComparator());
			
			StringBuilder sb = new StringBuilder();
			for(SaveDataBlock s : targets){
				if(sb.length() > 0){
					sb.append(";");
					sb.append(s.toString());
				}
				else
					sb.append(s.toString());
			}
			output.collect(new Text(), new Text(key.toString()+FIELD_SEPERATOR
							+sb.toString()));
			reporter.progress();
		}
		
		public class SaveDataBlock{
			public String userId;
			public long score;
			public int type;
			
			public SaveDataBlock(String userId, String score, String type) {
				this.userId = userId;
				this.score = NumberUtils.toLong(score);
				this.type = NumberUtils.toInt(type);
			}
			
			public void addData(String userId, String score, String type, int localtype) {
				int type_i = NumberUtils.toInt(type);
				if(localtype == FROM_INTERACTIVE){
					this.type |= type_i;
				}else if(localtype == FROM_PROFILE){
					this.type |= (1<<(type_i-1));
				}
				else if(localtype == FROM_SHOPPING){
					this.type |= (1<<(type_i-1));
				}
				else
					return;
				this.score += NumberUtils.toLong(score);
			}
			
			public String toString(){
				return userId+","+(int)score+","+type;
			}
		}
		
		public class SimpleComparator implements Comparator<SaveDataBlock>{
			@Override
			public int compare(SaveDataBlock o1, SaveDataBlock o2) {
				return (int)(o1.score - o2.score);
			}
		}
	}
	
	@Override
	public int run(String[] args) throws Exception {
		JobConf job = new JobConf(getConf(), ProfileCombine.class);
		job.setJarByClass(ProfileCombine.class);
		job.setJobName("combine all data, profile,interactive");
		job.setNumReduceTasks(400);
		job.set("mapred.child.java.opts","-Xmx1024m");
		job.set("mapred.job.queue.name", "cug-taobao-sns");
		job.set("io.sort.factor", "100");
		job.set("io.sort.mb", "512");

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		job.setReducerClass(ProfileCombineReducer.class);

//		job.setInputFormatClass(SequenceFileInputFormat.class);
		SequenceFileOutputFormat.setOutputCompressionType(job, SequenceFile.CompressionType.BLOCK);
		job.setOutputFormat(SequenceFileOutputFormat.class);

		// 由于zeus的时间参数不能正常运作,所以这里我们自己指定时间.
		// 替换 参数中的yyyyMMdd
		Calendar cal = Calendar.getInstance();
		cal.set(Calendar.DAY_OF_MONTH, cal.get(Calendar.DAY_OF_MONTH)-1);
		SimpleDateFormat s = new SimpleDateFormat("yyyyMMdd");
		String datepath = s.format(cal.getTime());

		// 合并所有的数据类型, 第一个是interactive,然后是购物数据,最后是profile数据
//		String shopPath = args[0].replaceAll("yyyyMMdd", datepath);
//		MultipleInputs.addInputPath(job, new Path(shopPath),
//				TextInputFormat.class, ShopMapper.class);
//		System.out.println(shopPath);
		String interactivePath = args[0].replaceAll("yyyyMMdd", datepath);
		MultipleInputs.addInputPath(job, new Path(interactivePath), 
				SequenceFileInputFormat.class, InteractiveMapper.class);
		System.out.println(interactivePath);
		String shopPath = args[1].replaceAll("yyyyMMdd", datepath);
		MultipleInputs.addInputPath(job, new Path(shopPath), 
				TextInputFormat.class, ShopMapper.class);
		System.out.println(shopPath);
		int profileCount = 0;
		for(int i=2; i<args.length-1; i++){
			String path = args[i].replaceAll("yyyyMMdd", datepath);
			MultipleInputs.addInputPath(job, new Path(path),
					SequenceFileInputFormat.class, ProfileMapper.class);
			System.out.println(path);
			profileCount++;
		}
		job.set("profileDataCount", ""+profileCount);
		
		// 最后一个是输出路径
		String outpath = args[args.length-1].replaceAll("yyyyMMdd", datepath);
		FileOutputFormat.setOutputPath(job, new Path(outpath));

		JobClient.runJob(job);
		return 0;
	}
	
	public static void main(String[] args) throws Exception {
		int status = ToolRunner.run(new Configuration(), new ProfileCombine(), args);
		System.exit(status);
	}

}
