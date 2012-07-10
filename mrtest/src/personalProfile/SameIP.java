package personalProfile;

import java.io.IOException;
import java.util.ArrayList;
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

public class SameIP extends Configured implements Tool {
	private final static int USER_ID = 0;
	private final static int USER_TB_IP = 1;
	private final static int MAX_USERS = 20000;
	
	public static class MapClass extends MapReduceBase implements
			Mapper<LongWritable, Text, Text, Text> {
		
		@Override
		public void map(LongWritable key, Text value,
				OutputCollector<Text, Text> output, Reporter reporter)
				throws IOException {
			// TODO Auto-generated method stub
			String str = value.toString();
			String[] snsUserIpTable = str.split("\t");
			
			System.out.println("str:[" + str + "]");
			String[] ip = snsUserIpTable[USER_TB_IP].split(",");
			
			for (int i = 0; i < ip.length; i++) {
				output.collect(new Text(ip[i]), new Text(snsUserIpTable[USER_ID]));
			}			
		}
		
	}
	
	public static class Reduce extends MapReduceBase implements
			Reducer<Text, Text, Text, Text> {

		@Override
		public void reduce(Text key, Iterator<Text> values,
				OutputCollector<Text, Text> output, Reporter reporter)
				throws IOException {
			// TODO Auto-generated method stub
			List<String> users = new ArrayList<String>();
			String fromUid = null;
			String toUid = null;
			int usersSize = -1;
			int limitSize = -1;
			
			while (values.hasNext()) {
				users.add(values.next().toString());
			}
			
			usersSize = users.size();
			if (usersSize > MAX_USERS) {
				return ;
			}
			System.out.println("" + key.toString() + " have " + usersSize + " users.");
			
			for (int i = 0; i < usersSize; i++) {
				fromUid = users.get(i);
				limitSize = 0;
				for (int j = 0; j < usersSize && limitSize < 1000; j++) {
					toUid = users.get(j);				
					if (fromUid.equals(toUid)) {
						continue;
					}
					limitSize++;
					output.collect(key, new Text(MyStringLib.combine("\001", fromUid,
									toUid, key.toString())));
				}
			}
		}
		
	}
	
	@Override
	public int run(String[] args) throws Exception {
		// TODO Auto-generated method stub
		Configuration conf = getConf();
		JobConf job = new JobConf(conf, SameIP.class);
		Path in = new Path(getInputPath()); // it is used on Zeus.
		Path out = new Path(System.getenv("instance.job.outputPath")); // it is used on Zeus.
		
		FileInputFormat.setInputPaths(job, in);
		FileOutputFormat.setOutputPath(job, out);
		
		job.setJobName("Same IP user...");
		
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
		String inputPath = "/group/taobao/taobao/hive/r_user_ip_result_search/" +
				"pt=" + MyDate.getLatestSunday() + "000000/";
		System.out.println("SameCellphone.getInputPath() 1" + inputPath);
		return inputPath;
	}
	public static void main(String[] args) throws Exception {
		int status = ToolRunner.run(new Configuration(), new SameIP(), args);
		System.exit(status);
	}

}
