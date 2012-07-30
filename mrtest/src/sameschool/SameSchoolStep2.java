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
 * �����ȷ����û���ѧУ�����ֲ�.
 * ����2012-07-12��������ʾ
 * 1	5476411
 * 2	2929827
 * 3	56504
 * 4	4801
 * 5	1277
 * 6	1
 * ��Ϊ�û������ѧУ������5��,6��������������.
 * 4-5��Ҳ����������. ���Կ���99%���ϵĶ���1-2�����û�.Ҳ����˵���������1000��,
 * ��ô��ͨ�û��ͻ��Ƴ�1000/500��ͬѧУ����.���ǰ�����������800/400
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
	// һ���û�����ͬѧУ��������
	private final static int MAX_SAME_SCHOOL = 800;
	private final static int MAX_SAME_SCHOOL_SINGLE = 200;
	//
	private final static String FIELD_SEPERATOR = "\001";
	private final static String s_s_spetator = "\002";
	private final static String s_t_sperator = "\003";

	
	public static class MapClass extends Mapper<Text, Text, Text, Text>{
		
		/**
		 * ��һ����������
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
			// һ���û�5��ѧУ.ÿ��ѧУ���1000��.������5000����¼.
			Map<String, Map<Long, String>> schools = new HashMap<String, Map<Long,String>>();
			// ��������ÿ������ļ���
			Map<String, Integer> schoolCounter = new HashMap<String, Integer>();
			// ���������Ѿ����е��û�,��Ҫ���кϲ�
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
			// ����һ���ǲ����ж����ͬѧУ�Ĵ���
			
			// �������,���ǲ�ȫ,��ǰ��ʣ�������������ĳ�ȡ����,ֱ������Ϊֹ
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

				// ����Ŀǰob����汾֮ǰ����double�ڲ�ͬ�İ汾�ϲ�һ�µ�����.
				// ����socre�ȳ���һ������Ȼ����int����
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

		// ����zeus��ʱ�����������������,�������������Լ�ָ��ʱ��.
		// �滻 �����е�yyyyMMdd
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
