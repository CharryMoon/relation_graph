package sameschool;


import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapred.FileInputFormat;
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
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import sameschool.SameSchoolStep2.MapClass;
import sameschool.SameSchoolStep2.Reduce;
import util.SimpleStringTokenizer;



public class SameSchool extends Configured implements Tool {
	private static final String SENIOR = "1";
	private static final int USER_ID = 0;	// 用户id
	private static final int SCHOOL_ID = 1;	// 学校id
	private static final int ENTER_DAY = 2;	// 入学时间
	private static final int CAREER = 3;	// (1, "高中"),(2,"大专"), (3, "大学"), (4, * "硕士"), (5, "博士");

	private static final int SCHOOL_DEPARTMENT = 9;	// 大学的院系,系统提供,用户选取
	private static final int SCHOOL_CLASS = 10;		// 当为高中的时候,由用户自由输入
	private static final int MAX_STUDENT = 20000;
	private static final int MAX_RECOMMENT = 1000;
	
	public static class MapClass extends MapReduceBase implements
			Mapper<LongWritable, Text, Text, Text> {		
		
		@Override
		public void map(LongWritable key, Text value,
				OutputCollector<Text, Text> output, Reporter reporter)
				throws IOException {
			SimpleStringTokenizer simpleStringTokenizer = new SimpleStringTokenizer(value.toString(), "\t", 11);
			List<String> fields = simpleStringTokenizer.getAllElements();
			String str = value.toString();
			StringBuilder valueStr = new StringBuilder();
			
			// 高中
			if(SENIOR.equals(fields.get(CAREER))){
				valueStr.append("1").append("\t");
				valueStr.append(fields.get(USER_ID)).append("\t");
				valueStr.append(fields.get(ENTER_DAY)).append("\t");
				valueStr.append(fields.get(SCHOOL_CLASS));
			}
			else{
				valueStr.append(fields.get(CAREER)).append("\t");
				valueStr.append(fields.get(USER_ID)).append("\t");
				valueStr.append(fields.get(ENTER_DAY)).append("\t");
				valueStr.append(fields.get(SCHOOL_DEPARTMENT));
			}
			
			output.collect(new Text(fields.get(SCHOOL_ID)), new Text(valueStr.toString()));
		}
		
	}
	
	public static class Reduce extends MapReduceBase implements
			Reducer<Text, Text, Text, Text> {
		@Override
		public void reduce(Text key, Iterator<Text> values,
				OutputCollector<Text, Text> output, Reporter reporter)
				throws IOException {
			
			// 存放无任何额外信息的用户
			List<Student> simpleSchoolInfo = new ArrayList<Student>();
			// 填写的入学时间
			List<Student> schoolInfo_enterDay = new ArrayList<Student>();
			// 填写了 班级或者院系信息
			List<Student> schoolInfo_extra = new ArrayList<Student>();
			// 填写了时间和院系
			List<Student> schoolInfo_day_extra = new ArrayList<Student>();
			
			Set<String> duplicateCheck = new HashSet<String>();
			int student_in_school_count=0;
			while (values.hasNext()) {
				Student s = convertToStudent(values.next().toString());
				if(duplicateCheck.contains(s.getUserId()))
					continue;
				duplicateCheck.add(s.getUserId());

				if(s.getType() == Student.BASIC)
					simpleSchoolInfo.add(s);
				else if(s.getType() == Student.WITHENTERDAY)
					schoolInfo_enterDay.add(s);
				else if(s.getType() == Student.WITHCLASS)
					schoolInfo_extra.add(s);
				else if(s.getType() == Student.FULL)
					schoolInfo_day_extra.add(s);
				
				student_in_school_count ++;
			}
			
			/**
			 * 对于没有任何额外信息的用户,推荐给他信息最完整的用户,是目前我们的策略.
			 * 从最完整开始推荐,不足的后面部分完整的补上
			 */
			for(Student s : simpleSchoolInfo){
				Set<String> matchedStudent = new HashSet<String>();
				matchStudentByInfoIntegrity(key, output, simpleSchoolInfo,
						schoolInfo_enterDay, schoolInfo_extra,
						schoolInfo_day_extra, s, matchedStudent);
			}
			/**
			 * 对于有填写了任何信息的用户,最优先的策略是寻找和他的额外信息匹配的用户.全部找过一遍以后,
			 * 才按照信息完整度进行推荐.
			 */
			for(Student s : schoolInfo_enterDay){
				Set<String> matchedStudent = new HashSet<String>();
				for (Student student : schoolInfo_day_extra) {
					if(matchedStudent.size() > MAX_RECOMMENT)
						break;
					if(s.getUserId().equals(student.getUserId()))
						continue;
					if(!s.getEnterDay().equals(student.getEnterDay()))
						continue;
					
					matchedStudent.add(student.getUserId());
					output.collect(new Text(s.getUserId()), new Text(makeOutputValue(key, student, true)));
				}
				for (Student student : schoolInfo_enterDay) {
					if(matchedStudent.size() > MAX_RECOMMENT)
						break;
					if(s.getUserId().equals(student.getUserId()))
						continue;
					if(!s.getEnterDay().equals(student.getEnterDay()))
						continue;
					
					matchedStudent.add(student.getUserId());
					output.collect(new Text(s.getUserId()), new Text(makeOutputValue(key, student, true)));
				}
				matchStudentByInfoIntegrity(key, output, simpleSchoolInfo,
						schoolInfo_enterDay, schoolInfo_extra,
						schoolInfo_day_extra, s, matchedStudent);
			}
			//
			for(Student s : schoolInfo_extra){
				Set<String> matchedStudent = new HashSet<String>();
				for (Student student : schoolInfo_day_extra) {
					if(matchedStudent.size() > MAX_RECOMMENT)
						break;
					if(s.getUserId().equals(student.getUserId()))
						continue;
					if(!s.getExtra().equals(student.getExtra()))
						continue;
					
					matchedStudent.add(student.getUserId());
					output.collect(new Text(s.getUserId()), new Text(makeOutputValue(key, student, true)));
				}
				for (Student student : schoolInfo_extra) {
					if(matchedStudent.size() > MAX_RECOMMENT)
						break;
					if(s.getUserId().equals(student.getUserId()))
						continue;
					if(!s.getExtra().equals(student.getExtra()))
						continue;
					
					matchedStudent.add(student.getUserId());
					output.collect(new Text(s.getUserId()), new Text(makeOutputValue(key, student, true)));
				}
				matchStudentByInfoIntegrity(key, output, simpleSchoolInfo,
						schoolInfo_enterDay, schoolInfo_extra,
						schoolInfo_day_extra, s, matchedStudent);
			}
			//
			for(Student s : schoolInfo_day_extra){
				Set<String> matchedStudent = new HashSet<String>();
				for (Student student : schoolInfo_day_extra) {
					if(matchedStudent.size() > MAX_RECOMMENT)
						break;
					if(s.getUserId().equals(student.getUserId()))
						continue;
					if(!s.getEnterDay().equals(student.getEnterDay()) 
							&& !s.getExtra().equals(student.getExtra()))
						continue;
					
					matchedStudent.add(student.getUserId());
					output.collect(new Text(s.getUserId()), new Text(makeOutputValue(key, student, true)));
				}
				for (Student student : schoolInfo_enterDay) {
					if(matchedStudent.size() > MAX_RECOMMENT)
						break;
					if(s.getUserId().equals(student.getUserId()))
						continue;
					if(!s.getEnterDay().equals(student.getEnterDay()))
						continue;
					
					matchedStudent.add(student.getUserId());
					output.collect(new Text(s.getUserId()), new Text(makeOutputValue(key, student, true)));
				}
				for (Student student : schoolInfo_extra) {
					if(matchedStudent.size() > MAX_RECOMMENT)
						break;
					if(s.getUserId().equals(student.getUserId()))
						continue;
					if(!s.getExtra().equals(student.getExtra()))
						continue;
					
					matchedStudent.add(student.getUserId());
					output.collect(new Text(s.getUserId()), new Text(makeOutputValue(key, student, true)));
				}
				matchStudentByInfoIntegrity(key, output, simpleSchoolInfo,
						schoolInfo_enterDay, schoolInfo_extra,
						schoolInfo_day_extra, s, matchedStudent);
			}
			
			
//			for (int i = 0; i < size; i++) {
//				studentA = students.get(i);
//
//				limitSize = 0;
//				for (int j = 0; j < size && limitSize < 1000; j++) {
//					studentB = students.get(j);
//					
//					if (studentA.getUserId().equals(studentB.getUserId())) {
//						continue;
//					}
//					
//					limitSize++;				
//					output.collect(key, new Text(MyStringLib.combine("\001",
//							studentA.getUserId(), studentB.getUserId(), key.toString())));					
//				}
//			}
		}

		private void matchStudentByInfoIntegrity(Text key,
				OutputCollector<Text, Text> output,
				List<Student> simpleSchoolInfo,
				List<Student> schoolInfo_enterDay,
				List<Student> schoolInfo_extra,
				List<Student> schoolInfo_day_extra, Student s,
				Set<String> matchedStudent) throws IOException {
			for (Student student : schoolInfo_day_extra) {
				if(findMatchedUser(key, output, s, matchedStudent, student))
					break;
			}
			for (Student student : schoolInfo_extra) {
				if(findMatchedUser(key, output, s, matchedStudent, student))
					break;
			}
			for (Student student : schoolInfo_enterDay) {
				if(findMatchedUser(key, output, s, matchedStudent, student))
					break;
			}
			for (Student student : simpleSchoolInfo) {
				if(findMatchedUser(key, output, s, matchedStudent, student))
					break;
			}
		}

		private boolean findMatchedUser(Text key,
				OutputCollector<Text, Text> output, Student s,
				Set<String> matchedStudent, Student student) throws IOException {
			if(matchedStudent.size() > MAX_RECOMMENT)
				return true;
			if(s.getUserId().equals(student.getUserId()))
				return false;
			if(matchedStudent.contains(student.getUserId()))
				return false;
			
			matchedStudent.add(student.getUserId());
			output.collect(new Text(s.getUserId()), new Text(makeOutputValue(key, student, false)));
			return false;
		}

		private String makeOutputValue(Text key, Student student, boolean isMatched) {
			int type = student.getType();
			if(isMatched)
				type |= Student.MATCHED;
			return student.getUserId()+"\t"+key+"\t"+type;
		}

		private Student convertToStudent(String string) {
			int level = 0;
			Student student = new Student();
			SimpleStringTokenizer simpleStringTokenizer = new SimpleStringTokenizer(string, "\t", 4);
			List<String> fields = simpleStringTokenizer.getAllElements();
			
			int type = Student.BASIC;
			student.setCareer(fields.get(0));
			student.setUserId(fields.get(1));
			if(StringUtils.isNotBlank(fields.get(2))){
				type |= student.WITHENTERDAY;
				student.setEnterDay(fields.get(2));
			}
			if(StringUtils.isNotBlank(fields.get(3))){
				type |= student.WITHCLASS;
				student.setExtra(fields.get(3));
			}
			student.setType(type);
//			level += "".equals(student.getEnterDay()) ? 0 : 1;
//			level += "".equals(student.getSchoolClass()) ? 0 : 1;
//			level += "".equals(student.getSchoolDepartment()) ? 0 : 1;
//			student.setLevel(level);
			
			return student;
		}
				
	}

	@Override
	public int run(String[] args) throws Exception {
		JobConf job = createSameSchoolStep1Job(args);
		JobClient.runJob(job);
		String[] params = new String[]{args[1], args[1]+"2"};
		job = createSameSchoolStep2Job(params);
		JobClient.runJob(job);
		
		return 0; 
	}

	private JobConf createSameSchoolStep1Job(String[] args) {
		Configuration conf = getConf();
		JobConf job = new JobConf(conf, SameSchool.class);
		
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		job.setJobName("same school user analysis step 1...");
		
		job.setMapperClass(SameSchool.MapClass.class);
		job.setReducerClass(SameSchool.Reduce.class);
		
		job.setInputFormat(TextInputFormat.class);
		job.setOutputFormat(SequenceFileOutputFormat.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.set("mapred.job.queue.name", "cug-taobao-sns");
		
		job.setBoolean("mapred.output.compress", true); // config the reduce output compress
		job.setClass("mapred.output.compression.codec", GzipCodec.class,
				CompressionCodec.class); //
		
		job.setNumReduceTasks(100);
		job.set("mapred.child.java.opts","-Xmx896m");
		
//		job.setCompressMapOutput(true); //config the map output for compress.
//		job.setMapOutputCompressorClass(GzipCodec.class);
		return job;
	}
	
	private JobConf createSameSchoolStep2Job(String[] args) {
		Configuration conf = getConf();
		JobConf job = new JobConf(conf, SameSchoolStep2.class);
		
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		job.setJobName("same school user analysis step 2...");
		
		job.setMapperClass(SameSchool.MapClass.class);
		job.setReducerClass(SameSchool.Reduce.class);
		
		job.setInputFormat(SequenceFileInputFormat.class);
		job.setOutputFormat(SequenceFileOutputFormat.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.set("mapred.job.queue.name", "cug-taobao-sns");
		
		job.setBoolean("mapred.output.compress", true); // config the reduce output compress
		job.setClass("mapred.output.compression.codec", GzipCodec.class,
				CompressionCodec.class); 
		
		job.setNumReduceTasks(100);
		job.set("mapred.child.java.opts","-Xmx896m");
//		job.setCompressMapOutput(true); //config the map output for compress.
//		job.setMapOutputCompressorClass(GzipCodec.class);
		return job;
	}

	public static void main(String[] args) throws Exception {
		int status = ToolRunner.run(new Configuration(), new SameSchool(), args);
		System.exit(status);
	}

}
