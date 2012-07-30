package sameschool;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.math.NumberUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.sun.istack.internal.Nullable;
import com.sun.org.apache.xml.internal.serializer.ToUnknownStream;

import samecompany.Employee;
import samecompany.SameCompanyStep2;
import util.SimpleStringTokenizer;

/**
 * 我们先分析用户的学校数量分布.
 * 根据2012-07-12的数据显示
 * 1	5476411
 * 2	2929827
 * 3	56504
 * 4	4801
 * 5	1277
 * 6	1
 * 因为用户的最大学校上限是5个,6个的是垃圾数据.
 * 4-5个也基本是垃圾. 可以看到99%以上的都是1-2个的用户.也就是说如果上限是1000个,
 * 那么普通用户就会推出1000/500个同学校的人.我们把总数调整到800/400
 * @author leibiao
 *
 */
public class SameSchoolStep2 extends Configured implements Tool {
	private final static double Wt = 5.0;
	private final static double Wk = 1-(double)7347986277.0/(double)32792266871.0;
//	private final static int FROM_USER_ID = 0;
	private final static int TO_USER_ID = 0;
	private final static int SCHOOL_ID = 1;
	private final static int MATCHTYPE = 2;
	private final static String TYPE = "13";
	// 一个用户最多的同学校数据条数
	private final static int MAX_SAME_SCHOOL = 800;
	private final static int MAX_SAME_SCHOOL_SINGLE = 200;
	//
	private final static String FIELD_SEPERATOR = "\001";
	private final static String s_s_spetator = "\002";
	private final static String s_t_sperator = "\003";

	
	public static class MapClass extends Mapper<Text, Text, Text, Text>{
		
		/**
		 * 上一步产生类似
		 * fromUserId	toUserId	schoolId	type
		 *
		 */
		@Override
		public void map(Text key, Text value, Context context) 
				throws IOException ,InterruptedException {
//			SimpleStringTokenizer simpleStringTokenizer = new SimpleStringTokenizer(value.toString(), "\t", 3);
//			List<String> fields = simpleStringTokenizer.getAllElements();
//			String fromIdToId = key.toString()+"_"+fields.get(0);
			context.write(key, value);			
		}
	}
	
	public static class  Reduce extends Reducer<Text, Text, Text, Text> {

		@Override
		public void reduce(Text key, Iterable<Text> values, Context context) 
				throws IOException ,InterruptedException {
			// 一个用户5个学校.每个学校最多1000条.最多会有5000条记录.
			Map<String, Map<Long, String>> schools = new HashMap<String, Map<Long,String>>();
			// 用来保存每个里面的计数
			Map<String, Integer> schoolCounter = new HashMap<String, Integer>();
			// 用来保存已经命中的用户,需要进行合并
			Map<Long, String> matchedUsers = new HashMap<Long, String>(100);
			int leftCounter = 0;
			Iterator<Text> itor = values.iterator();
			while (itor.hasNext()) {
				SimpleStringTokenizer simpleStringTokenizer = new SimpleStringTokenizer(itor.next().toString(), "\t", 4);
				List<String> fields = simpleStringTokenizer.getAllElements();
				Long toUid = NumberUtils.toLong(fields.get(TO_USER_ID), 0);
				if(toUid == 0)
					continue;
				String schoolId = fields.get(SCHOOL_ID);
				String matchType = fields.get(MATCHTYPE);
				
				if(schoolCounter.containsKey(schoolId)){
					if(schoolCounter.get(schoolId) >= MAX_SAME_SCHOOL_SINGLE){
						if(schools.containsKey(schoolId)){
							Map<Long, String> schoolMap = schools.get(schoolId);
							schoolMap.put(toUid, fields.get(MATCHTYPE));
						}
						else{
							Map<Long, String> schoolMap = new HashMap<Long, String>();
							schoolMap.put(toUid, fields.get(MATCHTYPE));
							schools.put(schoolId, schoolMap);
						}
						leftCounter ++;
					}
					else{
						if(putAndCombineUser(matchedUsers, matchType, toUid, schoolId))
							continue;
						schoolCounter.put(schoolId, schoolCounter.get(schoolId)+1);
					}
				}
				else{
					schoolCounter.put(schoolId, 1);
					putAndCombineUser(matchedUsers, matchType, toUid, schoolId);
				}
			}
			// 过滤一次是不是有多个共同学校的存在
			
			// 如果不足,考虑补全,从前面剩余的数据中随机的抽取数据,直到补满为止
			if(matchedUsers.size() < MAX_SAME_SCHOOL){
				if(matchedUsers.size() + leftCounter <= MAX_SAME_SCHOOL){
					for(String schoolId : schools.keySet()){
						Map<Long, String> map = schools.get(schoolId);
						for(Long uid : map.keySet()){
							putAndCombineUser(matchedUsers, map.get(uid), uid, schoolId);
						}
					}
				}
				// random take , not implement
				else{
					for(String schoolId : schools.keySet()){
						Map<Long, String> map = schools.get(schoolId);
						for(Long uid : map.keySet()){
							putAndCombineUser(matchedUsers, map.get(uid), uid, schoolId);
							if(matchedUsers.size() > MAX_SAME_SCHOOL)
								break;
						}
						if(matchedUsers.size() > MAX_SAME_SCHOOL)
							break;
					}
				}
			}

			for(Long uid : matchedUsers.keySet()){
				int count = 1;
				String[] storeedSchools = matchedUsers.get(uid).split(s_s_spetator);
				count = storeedSchools.length;
				double score = 0.0;
				int sonWeight = 0;
				for(String storeedSchool : storeedSchools){
					// type  matchtype
					String combinedType = storeedSchool.substring(storeedSchool.indexOf(s_t_sperator)+1);
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
				String type, Long toUid, String schoolId) {
			if(matchedUsers.containsKey(toUid)){
				String content = matchedUsers.get(toUid);
				String[] storeedSchools = content.split(s_s_spetator);
				for(String storeedSchool : storeedSchools){
					String sid = storeedSchool.substring(0, storeedSchool.indexOf(s_t_sperator));
					if(schoolId.equals(sid))
						return true;
				}
				matchedUsers.remove(toUid);
				content += s_s_spetator+schoolId+s_t_sperator+type;
				matchedUsers.put(toUid, content);
			}
			else{
				matchedUsers.put(toUid, schoolId+" "+type);
				
			}
			return false;
		}
		
	}
	
	@Override
	public int run(String[] args) throws Exception {
		Job job = new Job(getConf());
		job.setJarByClass(SameSchoolStep2.class);
		job.setJobName("same school step 2 ...");
		job.setNumReduceTasks(50);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		job.setMapperClass(sameschool.SameSchoolStep2.MapClass.class);
		job.setReducerClass(sameschool.SameSchoolStep2.Reduce.class);

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
		int status = ToolRunner.run(new Configuration(), new SameSchoolStep2(), args);
		System.exit(status);
	}

}
