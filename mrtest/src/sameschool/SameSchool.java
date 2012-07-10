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
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

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
//			String[] snsUserEducationTable = str.split("\t");
			
			// 高中
			if(SENIOR.equals(fields.get(CAREER))){
				valueStr.append("1");
				valueStr.append(fields.get(USER_ID)).append("\t");
				valueStr.append(fields.get(ENTER_DAY)).append("\t");
				valueStr.append(fields.get(SCHOOL_CLASS));
			}
			else{
				valueStr.append(fields.get(CAREER));
				valueStr.append(fields.get(USER_ID)).append("\t");
				valueStr.append(fields.get(ENTER_DAY)).append("\t");
				valueStr.append(fields.get(SCHOOL_DEPARTMENT));
			}
			
//			if (snsUserEducationTable.length == 9) {
//				valueStr = MyStringLib.combine("\t", snsUserEducationTable[CAREER], "",
//									  snsUserEducationTable[ENTER_DAY],
//									  snsUserEducationTable[USER_ID]);				
//			} else if (snsUserEducationTable.length == 10) { // may appear exception.
//				if (SENIOR.equals(snsUserEducationTable[CAREER])) {
//					System.out.println("error happen..." + str + "end");
//				}
//				valueStr = MyStringLib.combine("\t", snsUserEducationTable[CAREER],
//									  snsUserEducationTable[SCHOOL_DEPARTMENT],
//									  snsUserEducationTable[ENTER_DAY],
//									  snsUserEducationTable[USER_ID]);
//			} else if (snsUserEducationTable.length == 11) {
//				if (!SENIOR.equals(snsUserEducationTable[CAREER])) {
//					System.out.println("error happen in here..." + str + "end");
//				}
//				valueStr = MyStringLib.combine("\t", snsUserEducationTable[CAREER],
//									  snsUserEducationTable[SCHOOL_CLASS],
//									  snsUserEducationTable[ENTER_DAY],
//									  snsUserEducationTable[USER_ID]);		
//			}						
			output.collect(new Text(fields.get(SCHOOL_ID)), new Text(valueStr.toString()));
		}
		
	}
	
	public static class Reduce extends MapReduceBase implements
			Reducer<Text, Text, Text, Text> {
		@Override
		public void reduce(Text key, Iterator<Text> values,
				OutputCollector<Text, Text> output, Reporter reporter)
				throws IOException {
			List<Student> students = new ArrayList<Student>();
			Student studentA = null;
			Student studentB = null;
			int size = -1;
			int limitSize;
			
			// 存放无任何额外信息的用户
			List<Student> simpleSchoolInfo = new ArrayList<Student>();
			// 填写的入学时间
			List<Student> schoolInfo_enterDay = new ArrayList<Student>();
			// 填写了 班级或者院系信息
			List<Student> schoolInfo_extra = new ArrayList<Student>();
			// 填写了时间和院系
			List<Student> schoolInfo_day_extra = new ArrayList<Student>();
			
			int student_in_school_count=0;
			while (values.hasNext()) {
				Student s = convertToStudent(values.next().toString());
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
//			if (size > MAX_STUDENT) {
//				return ;
//			}
//			Collections.sort(students, new ComparatorStudent());
			
			for(Student s : simpleSchoolInfo){
				int counter = 0;
				for (int i = 0; i < schoolInfo_day_extra.size(); i++) {
					if(++counter > MAX_RECOMMENT)
						break;
					output.collect(key, new Text());
				}
				for (int i = 0; i < schoolInfo_extra.size(); i++) {
					if(++counter > MAX_RECOMMENT)
						break;
					output.collect(key, new Text());
				}
				for (int i = 0; i < schoolInfo_enterDay.size(); i++) {
					if(++counter > MAX_RECOMMENT)
						break;
					output.collect(key, new Text());
				}
			}
			
			for (int i = 0; i < size; i++) {
				studentA = students.get(i);

				limitSize = 0;
				for (int j = 0; j < size && limitSize < 1000; j++) {
					studentB = students.get(j);
					
					if (studentA.getUserId().equals(studentB.getUserId())) {
						continue;
					}
					
					limitSize++;				
					output.collect(key, new Text(MyStringLib.combine("\001",
							studentA.getUserId(), studentB.getUserId(), key.toString())));					
				}
			}
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
		Configuration conf = getConf();
		JobConf job = new JobConf(conf, SameSchool.class);
		
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		job.setJobName("same school user analysis step 1...");
		
		job.setMapperClass(MapClass.class);
		job.setReducerClass(Reduce.class);
		
		job.setInputFormat(TextInputFormat.class);
		job.setOutputFormat(SequenceFileOutputFormat.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.set("mapred.job.queue.name", "cug-taobao-sns");
		
//		job.setBoolean("mapred.output.compress", true); // config the reduce output compress
//		job.setClass("mapred.output.compression.codec", GzipCodec.class,
//				CompressionCodec.class); //
		
		job.setNumReduceTasks(100);
		job.set("mapred.child.java.opts","-Xmx896m");
		
		job.setCompressMapOutput(true); //config the map output for compress.
		job.setMapOutputCompressorClass(GzipCodec.class);
		JobClient.runJob(job);
		
		return 0; 
	}
	
	public static void main(String[] args) throws Exception {
		int status = ToolRunner.run(new Configuration(), new SameSchool(), args);
		System.exit(status);
	}

}
