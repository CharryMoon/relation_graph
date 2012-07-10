package personalProfile;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

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


public class SameCellphone extends Configured implements Tool {
	private final static int USER_ID = 0;
	private final static int PHONE = 1;
	private final static int MAX_USERS = 100;
	private final static int PHONE_LENGTH = 11;
	
	public static class MapClass extends MapReduceBase implements
			Mapper<LongWritable, Text, Text, Text> {
		
		@Override
		public void map(LongWritable key, Text value,
				OutputCollector<Text, Text> output, Reporter reporter)
				throws IOException {
			// TODO Auto-generated method stub
			String str = value.toString();
			String[] snsUserIpTable = str.split("\t");
			String[] phones = snsUserIpTable[PHONE].split(",");
//			
//			System.out.println("value:" + str + ":");
//			System.out.println("phones's size:" + phones.length);			
			for (int i = 0; i < phones.length; i++) {
//				System.out.println("phones " + i + "[" + phones[i] + "]");
				if (!isValidCellphone(phones[i])) {
					continue;
				}
				output.collect(new Text(phones[i]),
						new Text(snsUserIpTable[USER_ID]));				
			}
			
		}

		private boolean isValidCellphone(String string) {
			// TODO Auto-generated method stub
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
	
	public static class Reduce extends MapReduceBase implements
			Reducer<Text, Text, Text, Text> {

		@Override
		public void reduce(Text key, Iterator<Text> values,
				OutputCollector<Text, Text> output, Reporter reporter)
				throws IOException {
			// TODO Auto-generated method stub
			Set<String> sets = new HashSet<String>();
			List<String> users = new ArrayList<String>();
			String userAId = null;
			String userBId = null;
			int usersSize = -1;
			
			while (values.hasNext()) {
				sets.add(values.next().toString());
			}
			usersSize = sets.size();
			if (usersSize > MAX_USERS) {
				return ;
			}	
			users.addAll(sets);
//			if (10000 < usersSize && usersSize < MAX_USERS) {
//				System.out.println("" + key.toString() + " have " + usersSize + " users.");
//			}
			
			for (int i = 0; i < usersSize; i++) {
				userAId = users.get(i);
				for (int j = 0; j < usersSize; j++) {
					userBId = users.get(j);					
					if (userAId.equals(userBId)) {
						continue;
					}					
					output.collect(key, new Text(MyStringLib.combine("\001",
							userAId, userBId, key.toString())));
				}
			}
		}
		
	}
	
	@Override
	public int run(String[] args) throws Exception {
		// TODO Auto-generated method stub
		Configuration conf = getConf();
		JobConf job = new JobConf(conf, SameCellphone.class);
		Path in = new Path(getInputPath()); // it is used on Zeus.
		Path out = new Path(System.getenv("instance.job.outputPath")); // it is used on Zeus.
		
		FileInputFormat.setInputPaths(job, in);
		FileOutputFormat.setOutputPath(job, out);
		
		job.setJobName("Same cellphone user...");
		
		job.setMapperClass(MapClass.class);
		job.setReducerClass(Reduce.class);
		
		job.setInputFormat(TextInputFormat.class);
		job.setOutputFormat(SequenceFileOutputFormat.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.set("mapred.job.queue.name", "cug-taobao-sns");
		
		job.setBoolean("mapred.output.compress", true); // config the reduce output compress
		job.setClass("mapred.output.compression.codec", GzipCodec.class,
				CompressionCodec.class); 
		
		job.setNumReduceTasks(100);
//		job.setCompressMapOutput(true); //config the map output for compress.
//		job.setMapOutputCompressorClass(GzipCodec.class);
		JobClient.runJob(job);
		
		return 0;
	}
	
	private String getInputPath() {
		// TODO Auto-generated method stub
		String inputPath = "/group/taobao/taobao/hive/r_sns_deliver_address_final/" +
				"pt=" + MyDate.getLatestSunday() + "000000/";
		System.out.println("SameCellphone.getInputPath() 1" + inputPath);
		return inputPath;
	}

	public static void main(String[] args) throws Exception {
		int status = ToolRunner.run(new Configuration(), new SameCellphone(), args);
		System.exit(status);
	}

}
