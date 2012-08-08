package total;

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

import org.apache.commons.lang.math.NumberUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import util.SimpleStringTokenizer;


public class RealtionGraphCombine extends Configured implements Tool {
	// 默认分隔符
	private final static String FIELD_SEPERATOR = "\001";
	
	public static class MapClass extends Mapper<BytesWritable, Text, Text, Text> {
		private final static int FROM_USER_ID = 0;
		private final static int TO_USER_ID = 1;
		private final static int TYPE_INDEX = 2;
		private final static int SCORE_INDEX = 3;
		
		@Override
		public void map(BytesWritable key, Text value, Context context)
				throws IOException ,InterruptedException {
			List<String> fields = new SimpleStringTokenizer(value.toString(), FIELD_SEPERATOR).getAllElements();
			if(NumberUtils.toLong(fields.get(FROM_USER_ID), 0) == 0)
				return;
			if(NumberUtils.toLong(fields.get(TO_USER_ID), 0) == 0)
				return;
			
			context.write(new Text(fields.get(FROM_USER_ID)+"_"+fields.get(TO_USER_ID)), 
					new Text(fields.get(TYPE_INDEX)+FIELD_SEPERATOR+fields.get(SCORE_INDEX)));
		}
	}
		
	public static class Reduce extends Reducer<Text, Text, Text, Text> {
		private final static int TYPE_INDEX = 0;
		private final static int SCORE = 1;
		
		@Override
		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException ,InterruptedException {
			Iterator<Text> itor = values.iterator();
			int combineType = 0;
			double score = 0.0;
			while (itor.hasNext()) {
				List<String> fields = new SimpleStringTokenizer(itor.next().toString(), FIELD_SEPERATOR).getAllElements();
				int type = NumberUtils.toInt(fields.get(TYPE_INDEX), 0);
				if(type == 0)
					continue;
				combineType |= type;
				score += NumberUtils.toDouble(fields.get(SCORE), 0)/2;
			}
			
			String fromUid = "";
			String toUid = "";
			String uids[] = key.toString().split("_");
			fromUid = uids[0];
			toUid = uids[1];
			if(combineType == 0)
				return;
			
			context.write(new Text(fromUid+FIELD_SEPERATOR
					+toUid +FIELD_SEPERATOR
					+combineType +FIELD_SEPERATOR
					+score), new Text());
			context.progress();
		}
	}
	
	@Override
	public int run(String[] args) throws Exception {
		Job job = new Job(getConf());
		job.setJarByClass(RealtionGraphCombine.class);
		job.setJobName("combine interactive and profile ");
		job.setNumReduceTasks(200);
		job.getConfiguration().set("mapred.child.java.opts","-Xmx1024m");
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

		// 合并所有的profile的所有数据类型
		for(int i=0; i<args.length-1; i++){
			String path = args[i].replaceAll("yyyyMMdd", datepath);
			FileInputFormat.addInputPath(job, new Path(path));
			System.out.println(path);
		}

		// 最后一个是输出路径
		String outpath = args[args.length-1].replaceAll("yyyyMMdd", datepath);
		FileOutputFormat.setOutputPath(job, new Path(outpath));

		boolean success = job.waitForCompletion(true);
		return success ? 0 : 1;
	}
	
	public static void main(String[] args) throws Exception {
		int status = ToolRunner.run(new Configuration(), new RealtionGraphCombine(), args);
		System.exit(status);
	}

}
