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
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import sameschool.Student;
import util.SimpleStringTokenizer;

/**
 * 取BI计算得到的共同关注的数据,然后转换成我们使用的格式.保存下来
 * 完全的转换,无考虑数据上限等问题
 */
public class CopyOfCommonFollow extends Configured implements Tool {
	private final static String TYPE = "10";
	private final static int	USERID  = 2;
	private final static int	TARGETID  = 3;
	private final static int	TWOWAYCOUNT  = 4;
	private final static int	TWOWAY  = 5;
	private final static int	ONEWAYCOUNT  = 6;
	private final static int	ONEWAY  = 7;
	private final static int	MIXWAYCOUNT  = 8;
	private final static int	MIXWAY	= 9;
	private final static double Wt = 8.0;
	private final static double Wk = 1-(double)640162400.0/(double)32792266871.0;
	// 默认分隔符
	private final static String FIELD_SEPERATOR = "\001";
	private final static String CONTENT_SEPERATOR = "\002";
	// 关注的临界值,如果大于这个值,表示可信度很大,小于则大大降低这个可信度
	private final static int trust_num_valve = 10;
	
	public static class CommonFollowMapper extends Mapper<BytesWritable, Text, Text, Text> {

		protected void map(BytesWritable key, Text value, Context context)
				throws IOException ,InterruptedException {
//			context.setStatus(value.toString()+value.toString().split("\001")[0]);
			List<String> fields = new SimpleStringTokenizer(value.toString(), FIELD_SEPERATOR, 10).getAllElements();
			if(fields.size()  < 10){
				context.setStatus(fields.get(0)+" "+fields.get(1)+" " +fields.get(2)+" " +fields.get(3)+" "+fields.get(4));
				return;
			}
			
			int count = NumberUtils.toInt(fields.get(TWOWAYCOUNT), 0);
			double sonWeight = 0;
			String ids = "";
			if(count > 0){
				ids += fields.get(TWOWAY);
				sonWeight += count;
			}
			if(NumberUtils.toInt(fields.get(ONEWAYCOUNT), 0) > 0){
				count += NumberUtils.toInt(fields.get(ONEWAYCOUNT), 0);
				sonWeight += NumberUtils.toInt(fields.get(ONEWAYCOUNT), 0)*0.5;
				if(ids.length() > 0)
					ids = ids + "," + fields.get(ONEWAY);
				else
					ids += fields.get(ONEWAY);
			}
			if(NumberUtils.toInt(fields.get(MIXWAYCOUNT), 0) > 0){
				count += NumberUtils.toInt(fields.get(MIXWAYCOUNT), 0);
				sonWeight += NumberUtils.toInt(fields.get(MIXWAYCOUNT), 0)*0.8;
				if(ids.length() > 0)
					ids = ids + "," + fields.get(MIXWAY);
				else
					ids += fields.get(MIXWAY);
			}
			ids = ids.replaceAll(",", CONTENT_SEPERATOR);
			if(sonWeight <= trust_num_valve){
				sonWeight = sonWeight/2;
			}
			
			
			double score = 0.0;
			double degreeWeight = NumberUtils.toDouble(context.getConfiguration().get("degreeWeight"), Wt);
			double distributeParam = (1-NumberUtils.toDouble(context.getConfiguration().get("distributeParam"), Wk));
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
			sb.append(ids);

			context.write(new Text(), new Text(sb.toString()));
		}
	}
	
	public int run(String[] args) throws Exception {
		Job job = new Job(getConf());
		job.setJarByClass(CopyOfCommonFollow.class);
		job.setJobName("common follow");
		job.setNumReduceTasks(0);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		job.setMapperClass(CommonFollowMapper.class);

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
		int ret = ToolRunner.run(new CopyOfCommonFollow(), args);
		System.exit(ret);
	}
	
}
