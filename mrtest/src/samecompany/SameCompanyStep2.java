package samecompany;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.math.NumberUtils;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import util.SimpleStringTokenizer;

public class SameCompanyStep2 extends Configured implements Tool {
	private final static double Wt = 3.0;
	private final static double Wk = 1-(double)79809888.0/(double)32792266871.0;
	private final static int TO_USER_ID = 0;
	private final static int COMPANY_NAME = 1;
	private final static int MATCHTYPE = 2;
	private final static String TYPE = "12";
	// 一个用户最多的同学校数据条数
	private final static int MAX_SAME_COMPANY = 800;
	private final static int MAX_SAME_COMPANY_SINGLE = 200;
	// 默认分隔符
	private final static String FIELD_SEPERATOR = "\001";
	private final static String c_c_spetator = "\002";
	private final static String c_t_sperator = "\003";
	
	public static class MapClass extends Mapper<Text, Text, Text, Text> {
		
		/**
		 * 上一步产生类似
		 * fromUserId	toUserId	companyname	type
		 *
		 */
		protected void map(Text key, Text value, Context context) 
				throws IOException ,InterruptedException {
			SimpleStringTokenizer simpleStringTokenizer = new SimpleStringTokenizer(value.toString(), "\t", 3);
			List<String> fields = simpleStringTokenizer.getAllElements();
			context.write(key, value);
		};
	}
	
	public static class Reduce extends Reducer<Text, Text, Text, Text> {

		protected void reduce(Text key, Iterable<Text> values, Context context) 
				throws IOException ,InterruptedException {
			// 一个用户5个公司.每个公司最多800条.最多会有4000条记录.
			Map<String, Map<Long, String>> companies = new HashMap<String, Map<Long,String>>();
			// 用来保存每个里面的计数
			Map<String, Integer> companyCounter = new HashMap<String, Integer>();
			// 用来保存已经命中的用户,需要进行合并
			Map<Long, String> matchedUsers = new HashMap<Long, String>(100);
			int leftCounter = 0;
			Iterator<Text> itor = values.iterator();
			while (itor.hasNext()) {
				SimpleStringTokenizer simpleStringTokenizer = new SimpleStringTokenizer(itor.next().toString(), "\t", 3);
				List<String> fields = simpleStringTokenizer.getAllElements();
				Long toUid = NumberUtils.toLong(fields.get(TO_USER_ID), 0);
				if(toUid == 0)
					continue;
				String companyName = fields.get(COMPANY_NAME);
				String matchType = fields.get(MATCHTYPE);
				
				if(companyCounter.containsKey(companyName)){
					if(companyCounter.get(companyName) >= MAX_SAME_COMPANY_SINGLE){
						if(companies.containsKey(companyName)){
							Map<Long, String> schoolMap = companies.get(companyName);
							schoolMap.put(toUid, fields.get(MATCHTYPE));
						}
						else{
							Map<Long, String> schoolMap = new HashMap<Long, String>();
							schoolMap.put(toUid, fields.get(MATCHTYPE));
							companies.put(companyName, schoolMap);
						}
						leftCounter ++;
					}
					else{
						if(putAndCombineUser(matchedUsers, matchType, toUid, companyName))
							continue;
						companyCounter.put(companyName, companyCounter.get(companyName)+1);
					}
				}
				else{
					companyCounter.put(companyName, 1);
					putAndCombineUser(matchedUsers, matchType, toUid, companyName);
				}
			}
			// 过滤一次是不是有多个共同学校的存在
			
			// 如果不足,考虑补全,从前面剩余的数据中随机的抽取数据,直到补满为止
			if(matchedUsers.size() < MAX_SAME_COMPANY){
				if(matchedUsers.size() + leftCounter <= MAX_SAME_COMPANY){
					for(String schoolId : companies.keySet()){
						Map<Long, String> map = companies.get(schoolId);
						for(Long uid : map.keySet()){
							putAndCombineUser(matchedUsers, map.get(uid), uid, schoolId);
						}
					}
				}
				// random take , not implement
				else{
					for(String schoolId : companies.keySet()){
						Map<Long, String> map = companies.get(schoolId);
						for(Long uid : map.keySet()){
							putAndCombineUser(matchedUsers, map.get(uid), uid, schoolId);
							if(matchedUsers.size() > MAX_SAME_COMPANY)
								break;
						}
						if(matchedUsers.size() > MAX_SAME_COMPANY)
							break;
					}
				}
			}

			for(Long uid : matchedUsers.keySet()){
				int count = 1;
				String[] storeedCompanies = matchedUsers.get(uid).split(c_c_spetator);
				count = storeedCompanies.length;
				double score = 0.0;
				int sonWeight = 0;
				for(String storeedCompany : storeedCompanies){
					// type  matchtype
					String combinedType = storeedCompany.substring(storeedCompany.indexOf(c_t_sperator)+1);
					int[] types = Employee.getSplitType(combinedType);
					sonWeight += Integer.bitCount(types[0]);
					sonWeight += Integer.bitCount(types[1]);
				}
				double degreeWeight = NumberUtils.toDouble(context.getConfiguration().get("degreeWeight"), Wt);
				double distributeParam = (1-NumberUtils.toDouble(context.getConfiguration().get("distributeParam"), Wk));
				score = Math.sqrt(sonWeight)*degreeWeight*distributeParam/20;

				// 由于目前ob多个版本之前会有double在不同的版本上不一致的问题.
				// 所以socre先乘以一个大叔然后用int保存
				int mscore = (int)(score * 100000);
				context.write(new Text(), new Text(key +FIELD_SEPERATOR
											+TYPE +FIELD_SEPERATOR
											+ uid +FIELD_SEPERATOR
											+ count +FIELD_SEPERATOR
											+ mscore +FIELD_SEPERATOR
											+ matchedUsers.get(uid)));
			}
		}

		private boolean putAndCombineUser(Map<Long, String> matchedUsers,
				String type, Long toUid, String companyName) {
			if(matchedUsers.containsKey(toUid)){
				String content = matchedUsers.get(toUid);
				// name type█name type█name type
				String[] storeedCompanies = content.split(c_c_spetator);// substring(0, content.indexOf("█"));
				for(String storeedCompany : storeedCompanies){
					String cname = storeedCompany.substring(0, storeedCompany.indexOf(c_t_sperator));
					if(companyName.equals(cname))
						return true;
				}
				matchedUsers.remove(toUid);
				content += c_c_spetator+companyName+c_t_sperator+type;
				matchedUsers.put(toUid, content);
			}
			else{
				matchedUsers.put(toUid, companyName+c_t_sperator+type);
			}
			return false;
		}
	}

	public int run(String[] args) throws Exception {
		Job job = new Job(getConf());
		job.setJarByClass(SameCompanyStep2.class);
		job.setJobName("same company step 2 ...");
		job.setNumReduceTasks(50);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		job.setMapperClass(SameCompanyStep2.MapClass.class);
		job.setReducerClass(SameCompanyStep2.Reduce.class);

		job.setInputFormatClass(SequenceFileInputFormat.class);
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

		job.getConfiguration().set("degreeWeight", args[2]);
		job.getConfiguration().set("distributeParam", args[3]);
		boolean success = job.waitForCompletion(true);
		return success ? 0 : 1;
	}

	public static void main(String[] args) throws Exception {
		int ret = ToolRunner.run(new SameCompanyStep2(), args);
		System.exit(ret);
	}
}
