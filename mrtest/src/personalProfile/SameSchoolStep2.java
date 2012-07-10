package personalProfile;

import java.io.IOException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
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
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


public class SameSchoolStep2 extends Configured implements Tool {
	private final static int FROM_USER_ID = 0;
	private final static int TO_USER_ID = 1;
	private final static int SCHOOL_ID = 2;
	private final static int MAX_SCHOOL = 1000;
	private final static String TYPE = "13";
	
	public static class MapClass extends MapReduceBase implements
			Mapper<Text, Text, Text, Text> {
		
		@Override
		public void map(Text key, Text value,
				OutputCollector<Text, Text> output, Reporter reporter)
				throws IOException {
			// TODO Auto-generated method stub
			String fromIdToId = null;
			String str = value.toString();
			String[] snsUserSameSchoolTable = str.split("\001");
						
//			System.out.println("str:[" + str + "]");
//			System.out.println("snsTableLength:[" + snsUserSameCellphoneTable.length + "]");
			fromIdToId = MyStringLib.combineForKey(20, snsUserSameSchoolTable[FROM_USER_ID],
					snsUserSameSchoolTable[TO_USER_ID]);
			output.collect(new Text(fromIdToId), new Text(snsUserSameSchoolTable[SCHOOL_ID]));
			
		}
		
	}
	
	public static class Reduce extends MapReduceBase implements
			Reducer<Text, Text, Text, Text> {

		@Override
		public void reduce(Text key, Iterator<Text> values,
				OutputCollector<Text, Text> output, Reporter reporter)
				throws IOException {
			// TODO Auto-generated method stub
			String str = key.toString();
			String value1 = null;
			String fromUid = null;
			String toUid = null;
			Set<String> schoolIds = new HashSet<String>();
			
			while (values.hasNext()) {
				schoolIds.add(values.next().toString());
				if (schoolIds.size() >= MAX_SCHOOL) {
					break;
				}
			}
			value1 = MyStringLib.merge(schoolIds, ",");			
			fromUid = MyStringLib.removeZeroHead(str.substring(0, 20));
			toUid = MyStringLib.removeZeroHead(str.substring(20, 40));		
			output.collect(key, new Text(MyStringLib.combine("\001", fromUid, toUid,
					TYPE, String.valueOf(schoolIds.size()), value1)));
		}
		
	}
	
	@Override
	public int run(String[] args) throws Exception {
		// TODO Auto-generated method stub
		Configuration conf = getConf();
		JobConf job = new JobConf(conf, SameSchoolStep2.class);
		Path in = new Path(getInputPath()); // it is used on Zeus.
		Path out = new Path(System.getenv("instance.job.outputPath")); // it is used on Zeus.
		
		FileInputFormat.setInputPaths(job, in);
		FileOutputFormat.setOutputPath(job, out);
		
		job.setJobName("Same school step2 user...");
		
		job.setMapperClass(MapClass.class);
		job.setReducerClass(Reduce.class);
		
		job.setInputFormat(SequenceFileInputFormat.class);
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
		String inputPath = "/group/taobao-sns/yeshao.yxq/SameSchoolGziped/";
		System.out.println("SameSchoolStep2.getInputPath():" + inputPath);
		return inputPath;
	}

	public static void main(String[] args) throws Exception {
		int status = ToolRunner.run(new Configuration(), new SameSchoolStep2(), args);
		System.exit(status);
	}

}
