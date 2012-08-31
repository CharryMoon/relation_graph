package follow;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.math.NumberUtils;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.InputFormat;
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
import org.apache.hadoop.mapred.lib.MultipleInputs;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import sameschool.SameSchool;
import sameschool.Student;
import util.SimpleStringTokenizer;

/**
 * 取BI计算得到的共同关注的数据,然后转换成我们使用的格式.保存下来
 * 完全的转换,无考虑数据上限等问题
 */
public class IndirectCommonFollow extends Configured implements Tool {
	private final static String TYPE = "11";
	private final static int	USERID  = 2;
	private final static int	TARGETID  = 3;
	private final static int	COUNT  = 4;
	private final static int	FRIEND_IDS  = 5;
	private final static int	ONEWAYCOUNT  = 6;
	private final static int	FIELD_MAX  = 7;
	private final static double Wt = 6.0;
	private final static double Wk = 1-(double)167821402.0/(double)32792266871.0;
	// 默认分隔符
	private final static String FIELD_SEPERATOR = "\001";
	private final static String CONTENT_SEPERATOR = "\002";
	// 关注的临界值,如果大于这个值,表示可信度很大,小于则大大降低这个可信度
	private final static int trust_num_valve = 10;
	private static final int fromsource = 1;
	private static final int fromfilter = 2;
	private static int fromtype = 1;
	
	public static class IndirectCommonFollowMapper extends MapReduceBase implements
		Mapper<BytesWritable, Text, Text, Text> {
		private static double degreeWeight = Wt;
		private static double distributeParam = Wk;
		
		@Override
		public void configure(JobConf job) {
			degreeWeight = NumberUtils.toDouble(job.get("degreeWeight"), Wt);
			distributeParam = (1-NumberUtils.toDouble(job.get("distributeParam"), Wk));
			super.configure(job);
		}
		@Override
		public void map(BytesWritable key, Text value,
				OutputCollector<Text, Text> output, Reporter reporter)
				throws IOException {
			List<String> fields = new SimpleStringTokenizer(value.toString(), FIELD_SEPERATOR, FIELD_MAX).getAllElements();
			if(fields.size()  < FIELD_MAX){
//				context.setStatus(fields.get(0)+" "+fields.get(1)+" " +fields.get(2)+" " +fields.get(3)+" "+fields.get(4));
				return;
			}
			
			int count = NumberUtils.toInt(fields.get(COUNT), 0);
			double score = 0.0;
			double sonWeight = 0.0;
			sonWeight = count;
			sonWeight += NumberUtils.toInt(fields.get(ONEWAYCOUNT), 0)*0.5;
			if(sonWeight <= trust_num_valve){
				sonWeight = sonWeight/2;
			}
			score = Math.sqrt(sonWeight)*degreeWeight*distributeParam/20;
			// 由于目前ob多个版本之前会有double在不同的版本上不一致的问题.
			// 所以socre先乘以一个大叔然后用int保存
			int mscore = (int)(score * 100000);

			StringBuilder sb = new StringBuilder();
			sb.append(fields.get(USERID)).append(FIELD_SEPERATOR);
			sb.append(TYPE).append(FIELD_SEPERATOR);
			sb.append(fields.get(TARGETID)).append(FIELD_SEPERATOR);
			sb.append(count).append(FIELD_SEPERATOR);
			sb.append(mscore).append(FIELD_SEPERATOR);
			sb.append(fields.get(FRIEND_IDS).replaceAll(",", CONTENT_SEPERATOR));

//			context.write(new Text(), new Text(sb.toString()));
			output.collect(new Text(fields.get(USERID)), new Text(fromsource+"\t"+sb.toString()));
		}
	}
	public static class UicFilterMapper extends MapReduceBase implements
		Mapper<LongWritable, Text, Text, Text> {
	
		@Override
		public void map(LongWritable key, Text value, 
				OutputCollector<Text, Text> output, Reporter reporter)
				throws IOException {
			List<String> fields = new SimpleStringTokenizer(value.toString(), "\001").getAllElements();
			output.collect(new Text(fields.get(0)), new Text(fromfilter+"\t"));
		}
	}

	public static class IndirectCommonFollowReduce extends MapReduceBase implements
			Reducer<Text, Text, Text, Text> {
		@Override
		public void reduce(Text key, Iterator<Text> values,
				OutputCollector<Text, Text> output, Reporter reporter)
				throws IOException {
			List<String> sourceContent = new ArrayList<String>();
			while (values.hasNext()) {
				String value = values.next().toString();
//				System.out.println("key: "+key.toString()+",  value: "+value);
				int index = value.indexOf("\t");
				if(index == -1)
					continue;
				
				int type = Integer.valueOf(value.substring(0,index));
				if(type == fromsource)
					sourceContent.add(value.substring(index+1));
				else if(type == fromfilter){
//					 System.out.println("filter user:"+value);
					 return;
				}
			}
			for(String str : sourceContent){
				output.collect(new Text(), new Text(str));
			}
		}
	}
	
	public int run(String[] args) throws Exception {
		JobConf job = new JobConf(getConf(), IndirectCommonFollow.class);
		job.setJarByClass(IndirectCommonFollow.class);
		job.setJobName("indirect common follow");
		job.setNumReduceTasks(5);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

//		job.setMapperClass(IndirectCommonFollowMapper.class);
		job.setReducerClass(IndirectCommonFollowReduce.class);

//		job.setInputFormatClass(SequenceFileInputFormat.class);
		SequenceFileOutputFormat.setOutputCompressionType(job, SequenceFile.CompressionType.BLOCK);
		job.setOutputFormat(SequenceFileOutputFormat.class);

		// 由于zeus的时间参数不能正常运作,所以这里我们自己指定时间.
		// 替换 参数中的yyyyMMdd
		Calendar cal = Calendar.getInstance();
		cal.set(Calendar.DAY_OF_MONTH, cal.get(Calendar.DAY_OF_MONTH)-1);
		SimpleDateFormat s = new SimpleDateFormat("yyyyMMdd");
		String datepath = s.format(cal.getTime());
		
		// input path
		String spath = args[0].replaceAll("yyyyMMdd", datepath);
		job.setStrings("source_filename", spath);
		MultipleInputs.addInputPath(job, new Path(spath), 
				SequenceFileInputFormat.class, IndirectCommonFollowMapper.class);
		System.out.println(spath);
		String fpaths = args[1].replaceAll("yyyyMMdd", datepath);
		job.setStrings("filter_filename", fpaths);
		for(String fpath : fpaths.split(",")){
			MultipleInputs.addInputPath(job, new Path(fpath), 
					TextInputFormat.class, UicFilterMapper.class);
			System.out.println(fpath);
		}

		String outpath = args[2].replaceAll("yyyyMMdd", datepath);
		FileOutputFormat.setOutputPath(job, new Path(outpath));

		job.set("degreeWeight", args[3]);
		job.set("distributeParam", args[4]);
		JobClient.runJob(job);
		return 0;
	}
	
	public static void main(String[] args) throws Exception {
		int ret = ToolRunner.run(new IndirectCommonFollow(), args);
		System.exit(ret);
	}
	
}
