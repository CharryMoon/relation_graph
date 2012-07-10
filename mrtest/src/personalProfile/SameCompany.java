package personalProfile;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
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

public class SameCompany extends Configured implements Tool {
	private static final int USER_ID = 1;
	private static final int COMPANY_NAME = 2;
	private static final int BUSINESS_1 = 4;
	private static final int BUSINESS_2 = 5;
	private static final int POSITION_1 = 9;
	private static final int POSITION_2 = 10;
	private static final int MAX_STAFF = 20000;
	private static final int LIMIT_STAFF = 1000;
	
	public static class MapClass extends MapReduceBase implements
			Mapper<LongWritable, Text, Text, Text> {		
		
		@Override
		public void map(LongWritable key, Text value,
				OutputCollector<Text, Text> output, Reporter reporter)
				throws IOException {
			// TODO Auto-generated method stub
			String str = value.toString();
			String valueStr = "";
			String[] snsUserJobTable = str.split("\t");
			
			if ("".equals(snsUserJobTable[COMPANY_NAME])) {
				return ;
			}
			if (snsUserJobTable.length != 12) {
				System.out.println("debug***_ " + str + "end...");
				
				System.out.println("snsUserEducationLength: " + snsUserJobTable.length);
				for (int i = 0; i < snsUserJobTable.length; i++) {
					System.out.println("debug***__ " + i + " " + snsUserJobTable[i] + "end");
				}
			}
			
			valueStr = MyStringLib.combine("\001", snsUserJobTable[BUSINESS_1],
					snsUserJobTable[BUSINESS_2], snsUserJobTable[POSITION_1], 
					snsUserJobTable[POSITION_2], snsUserJobTable[USER_ID]);
			output.collect(new Text(snsUserJobTable[COMPANY_NAME]), new Text(valueStr));
		}		
		
	}
	
	public static class Reduce extends MapReduceBase implements
			Reducer<Text, Text, Text, Text> {
		
		@Override
		public void reduce(Text key, Iterator<Text> values,
				OutputCollector<Text, Text> output, Reporter reporter)
				throws IOException {
			// TODO Auto-generated method stub
			List<Staff> staffs = new ArrayList<Staff>();
			Staff staffA = null;
			Staff staffB = null;
//			String value1 = "";
			int staffsSize = -1;
			int limitSize = -1;
			
			while (values.hasNext()) {
				staffs.add(convertToStaff(values.next().toString()));
			}
			
			staffsSize = staffs.size();
			
			if (staffsSize > MAX_STAFF) { 
				return ;
			}
			
			if (staffsSize > 500) {
				System.out.println("company have " + staffsSize + " staffs...");
			}
			
			if (staffsSize > LIMIT_STAFF) {
				Collections.sort(staffs, new ComparatorStaff());
			}
			for (int i = 0; i < staffsSize; i++) {
				staffA = staffs.get(i);
				limitSize = 0;
				for (int j = 0; j < staffsSize && limitSize < 1000; j++) {
					staffB = staffs.get(j);			
					if (staffA.getUserId().equals(staffB.getUserId())) {
						continue;
					}
					limitSize++;
//					value1 = getCompareResult(staffA, staffB);
					
//					if ("".equals(staffA.getUserId())) {
//						System.out.println("error!!! staffA user id is empty.");
//					}
//					if ("".equals(staffB.getUserId())) {
//						System.out.println("error!!! staffB user id is empty");
//					}
//					if ("".equals(key.toString())) {
//						System.out.println("error!!! key is empty");
//					}
					
					output.collect(key, new Text(MyStringLib.combine("\001",
							staffA.getUserId(), staffB.getUserId(), key.toString())));
				}
			}			
			
		}

//		private String getCompareResult(Staff staffA, Staff staffB) {
//			// TODO Auto-generated method stub
//			String value1 = "";
//			
//			value1 += staffA.getBusiness1().equals(staffB.getBusiness1()) ? "Y" : "N";
//			value1 += staffA.getBusiness2().equals(staffB.getBusiness2()) ? "Y" : "N";
//			value1 += staffA.getPostion1().equals(staffB.getPostion1()) ? "Y" : "N";
//			value1 += staffA.getPostion2().equals(staffB.getPostion2()) ? "Y" : "N";
//			
//			return value1;
//		}

		private Staff convertToStaff(String string) {
			// TODO Auto-generated method stub
			int level = 0;
			Staff staff = new Staff();
			String[] strArray = string.split("\001");
			
			staff.setBusiness1(strArray[0]);
			staff.setBusiness2(strArray[1]);
			staff.setPostion1(strArray[2]);
			staff.setPostion2(strArray[3]);
			staff.setUserId(strArray[4]);
			level += "".equals(staff.getBusiness1()) ? 0 : 1;
			level += "".equals(staff.getBusiness2()) ? 0 : 1;
			level += "".equals(staff.getPostion1()) ? 0 : 1;
			level += "".equals(staff.getPostion2()) ? 0 : 1;
			staff.setLevel(level);
			
			return staff;
		}
		
	}
	
	@Override
	public int run(String[] args) throws Exception {
		// TODO Auto-generated method stub
		Configuration conf = getConf();
		JobConf job = new JobConf(conf, SameCompany.class);
		Path in = new Path(getInputPath()); // it is used on Zeus.
		Path out = new Path(System.getenv("instance.job.outputPath")); // it is used on Zeus.
		
		FileInputFormat.setInputPaths(job, in);
		FileOutputFormat.setOutputPath(job, out);
		
		job.setJobName("same company analysis...");
		
		job.setMapperClass(MapClass.class);
		job.setReducerClass(Reduce.class);
		
		job.setInputFormat(TextInputFormat.class);
		job.setOutputFormat(SequenceFileOutputFormat.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.set("mapred.job.queue.name", "cug-taobao-sns");
//		job.set("mapred.sequencefileoutputformat.separator", "\n");
		
		job.setBoolean("mapred.output.compress", true); // config the reduce output for compress
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
				+ "/sns_users_job";
		System.out.println("SameCompany.getInputPath():" + inputPath);
		return inputPath;
	}
	
	public static void main(String[] args) throws Exception {
		int status = ToolRunner.run(new Configuration(), new SameCompany(), args);
		System.exit(status);
	}

}
