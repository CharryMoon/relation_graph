package samephone;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.apache.commons.lang.math.NumberUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
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

import samecompany.Employee;
import samecompany.SameCompanyStep2;
import util.SimpleStringTokenizer;



public class SameCellphoneStep2 extends Configured implements Tool {
	private final static double Wt = 15.0;
	private final static double Wk = 1-(double)338957698.0/(double)32792266871.0;
	private final static int FROM_USER_ID = 0;
	private final static int TO_USER_ID = 1;
	private final static int VALUE = 2;
	private final static int MAX_USERS = 100;
	private final static String TYPE = "15";
	// 默认分隔符
	private final static String FIELD_SEPERATOR = "\001";
	
	public static class MapClass extends Mapper<Text, Text, Text, Text> {
		
		@Override
		public void map(Text key, Text value, Context context) 
				throws IOException ,InterruptedException {
			SimpleStringTokenizer simpleStringTokenizer = new SimpleStringTokenizer(value.toString(), "\t", 2);
			List<String> fields = simpleStringTokenizer.getAllElements();
						
			String fromIdToId = fields.get(FROM_USER_ID)+"_"+fields.get(TO_USER_ID);
			context.write(new Text(fromIdToId), key);
		}
		
	}
	
	public static class Reduce extends Reducer<Text, Text, Text, Text> {
		private static final int PHONE_COUNT_MAX = 10;
		
		@Override
		public void reduce(Text key, Iterable<Text> values,
				Context context) throws IOException, InterruptedException {
			Set<String> cellphones = new HashSet<String>();
			String value1 = null;

			Iterator<Text> itor = values.iterator();
			while (itor.hasNext()) {
				if (cellphones.size() > PHONE_COUNT_MAX) {
					break;
				}

				String phonenum = itor.next().toString();
				if(!cellphones.contains(phonenum))
					cellphones.add(phonenum);
			}
			
//			value1 = MyStringLib.merge(cellphones, ",");
			String ids[] = key.toString().split("_");

			
			double score = 0.0;
			int sonWeight = cellphones.size();
			double degreeWeight = NumberUtils.toDouble(context.getConfiguration().get("degreeWeight"), Wt);
			double distributeParam = (1-NumberUtils.toDouble(context.getConfiguration().get("distributeParam"), Wk));
			score = Math.sqrt(sonWeight)*degreeWeight*distributeParam/20;

			// 由于目前ob多个版本之前会有double在不同的版本上不一致的问题.
			// 所以socre先乘以一个大叔然后用int保存
			int mscore = (int)(score * 100000);

			context.write(new Text(), new Text(ids[0] +FIELD_SEPERATOR
												+TYPE +FIELD_SEPERATOR
												+ ids[1] +FIELD_SEPERATOR
												+ cellphones.size() +FIELD_SEPERATOR
												+ mscore +FIELD_SEPERATOR
												));
		}		
		
	}
	
	@Override
	public int run(String[] args) throws Exception {
		Job job = new Job(getConf());
		job.setJarByClass(SameCellphoneStep2.class);
		job.setJobName("Same cellphone step2 user...");
		job.setNumReduceTasks(100);
		job.getConfiguration().set("mapred.job.queue.name", "cug-taobao-sns");

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		job.setMapperClass(MapClass.class);
		job.setReducerClass(Reduce.class);

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
		int status = ToolRunner.run(new Configuration(), new SameCellphoneStep2(), args);
		System.exit(status);
	}

}
