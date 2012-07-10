package personalProfile;


import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

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



public class SameSchool extends Configured implements Tool {
	private static final String SENIOR = "1";
	private static final int USER_ID = 0;
	private static final int SCHOOL_ID = 1;
	private static final int ENTER_DAY = 2;
	private static final int CAREER = 3;
	private static final int SCHOOL_DEPARTMENT = 9;
	private static final int SCHOOL_CLASS = 10;
	private static final int MAX_STUDENT = 20000;
	
	public static class MapClass extends MapReduceBase implements
			Mapper<LongWritable, Text, Text, Text> {		
		
		@Override
		public void map(LongWritable key, Text value,
				OutputCollector<Text, Text> output, Reporter reporter)
				throws IOException {
			// TODO Auto-generated method stub
			String str = value.toString();
			String valueStr = "";
			String[] snsUserEducationTable = str.split("\t");
			
			if (snsUserEducationTable.length == 9) {
				valueStr = MyStringLib.combine("\t", snsUserEducationTable[CAREER], "",
									  snsUserEducationTable[ENTER_DAY],
									  snsUserEducationTable[USER_ID]);				
			} else if (snsUserEducationTable.length == 10) { // may appear exception.
				if (SENIOR.equals(snsUserEducationTable[CAREER])) {
					System.out.println("error happen..." + str + "end");
				}
				valueStr = MyStringLib.combine("\t", snsUserEducationTable[CAREER],
									  snsUserEducationTable[SCHOOL_DEPARTMENT],
									  snsUserEducationTable[ENTER_DAY],
									  snsUserEducationTable[USER_ID]);
			} else if (snsUserEducationTable.length == 11) {
				if (!SENIOR.equals(snsUserEducationTable[CAREER])) {
					System.out.println("error happen in here..." + str + "end");
				}
				valueStr = MyStringLib.combine("\t", snsUserEducationTable[CAREER],
									  snsUserEducationTable[SCHOOL_CLASS],
									  snsUserEducationTable[ENTER_DAY],
									  snsUserEducationTable[USER_ID]);		
			}						
			output.collect(new Text(snsUserEducationTable[SCHOOL_ID]),
					new Text(valueStr));
		}
		
	}
	
	public static class Reduce extends MapReduceBase implements
			Reducer<Text, Text, Text, Text> {
		@Override
		public void reduce(Text key, Iterator<Text> values,
				OutputCollector<Text, Text> output, Reporter reporter)
				throws IOException {
			// TODO Auto-generated method stub
			Set<String> studentsInformation = new HashSet<String>();
			List<Student> students = new ArrayList<Student>();
			Student studentA = null;
			Student studentB = null;
//			String value1 = "";
			int size = -1;
			int limitSize;
			
			while (values.hasNext()) {
				studentsInformation.add(values.next().toString());				
			}		
			size = studentsInformation.size();
			if (size > MAX_STUDENT) {
				return ;
			}
			for (String str : studentsInformation) {
				students.add(convertToStudent(str));
			}
			Collections.sort(students, new ComparatorStudent());
			
			for (int i = 0; i < size; i++) {
				studentA = students.get(i);
				
				limitSize = 0;
				for (int j = 0; j < size && limitSize < 1000; j++) {
					studentB = students.get(j);
					
					if (studentA.getUserId().equals(studentB.getUserId())) {
						continue;
					}
					
					limitSize++;				
//					value1 = getCompareResult(studentA, studentB);									
					output.collect(key, new Text(MyStringLib.combine("\001",
							studentA.getUserId(), studentB.getUserId(), key.toString())));					
				}
			}
		}

//		private String getCompareResult(Student studentA, Student studentB) {
//			// TODO Auto-generated method stub
//			String value1 = "";
//			
//			if (studentA.getCareer().equals(studentB.getCareer())) {
//				value1 = studentA.getCareer();
//				
//				if (!"".equals(studentA.getSchoolClass())) {
//					if (studentA.getSchoolClass().equals(studentB.getSchoolClass())) {
//						value1 += "Y";
//					} else {
//						value1 += "N";
//					}
//				} else if (!"".equals(studentA.getSchoolDepartment())){
//					if (studentA.getSchoolDepartment().equals(studentB.getSchoolDepartment())) {
//						value1 += "Y";
//					} else {
//						value1 += "N";
//					}
//				} else {
//					value1 += "N";
//				}
//				
//				if (!"".equals(studentA.getEnterDay()) 
//						&& studentA.getEnterDay().equals(studentB.getEnterDay())) {
//					value1 += "Y";
//				} else {
//					value1 += "N";
//				}
//			} else {
//				value1 = "NNN";
//			}
//			
//			return value1;
//		}

		private Student convertToStudent(String string) {
			// TODO Auto-generated method stub
			int level = 0;
			Student student = new Student();
			String[] strArray = string.split("\t");
			
			if (SENIOR.equals(strArray[0])) {
				student.setCareer("1");
				student.setSchoolClass(strArray[1]);
			} else {
				student.setCareer(strArray[0]);
				student.setSchoolDepartment(strArray[1]);
			}			
			student.setEnterDay(strArray[2]);
			student.setUserId(strArray[3]);			
			level += "".equals(student.getEnterDay()) ? 0 : 1;
			level += "".equals(student.getSchoolClass()) ? 0 : 1;
			level += "".equals(student.getSchoolDepartment()) ? 0 : 1;
			student.setLevel(level);
			
			return student;
		}
				
	}

	@Override
	public int run(String[] args) throws Exception {
		// TODO Auto-generated method stub
		Configuration conf = getConf();
		JobConf job = new JobConf(conf, SameSchool.class);
		Path in = new Path(getInputPath()); // it is used on Zeus.
		Path out = new Path(System.getenv("instance.job.outputPath")); // it is used on Zeus.
		
		FileInputFormat.setInputPaths(job, in);
		FileOutputFormat.setOutputPath(job, out);
		
		job.setJobName("same school user analysis...");
		
		job.setMapperClass(MapClass.class);
		job.setReducerClass(Reduce.class);
		
		job.setInputFormat(TextInputFormat.class);
		job.setOutputFormat(SequenceFileOutputFormat.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.set("mapred.job.queue.name", "cug-taobao-sns");
		
		job.setBoolean("mapred.output.compress", true); // config the reduce output compress
		job.setClass("mapred.output.compression.codec", GzipCodec.class,
				CompressionCodec.class); //
		
		job.setNumReduceTasks(100);
		
//		job.setCompressMapOutput(true); //config the map output for compress.
//		job.setMapOutputCompressorClass(GzipCodec.class);
		JobClient.runJob(job);
		
		return 0; 
	}
	
	private String getInputPath() {
		// TODO Auto-generated method stub
		String inputPath = "/group/taobao/taobao/dw/stb/" + MyDate.getDay(-1)
				+ "/sns_users_education";
		System.out.println("SameSchool.getInputPath():" + inputPath);
		return inputPath;
	}
	
	public static void main(String[] args) throws Exception {
		int status = ToolRunner.run(new Configuration(), new SameSchool(), args);
		System.exit(status);
	}

}
