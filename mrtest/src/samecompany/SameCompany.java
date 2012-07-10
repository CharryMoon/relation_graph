package samecompany;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.List;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import util.SimpleStringTokenizer;

public class SameCompany extends Configured implements Tool {
	/**
	 * 计算相同公司的用户
	 * 首先先把所有相同公司的用户放到同一个group中
	 * @author leibiao
	 *
	 */
	public static class SameCompanyMapper extends Mapper<LongWritable, Text, Text, Text> {
		private final int USER_ID = 1;
		private final int COMPANY_NAME = 2;
		private final int BUSINESS_1 = 4;
		private final int BUSINESS_2 = 5;
		private final int POSITION_1 = 6;
		private final int POSITION_2 = 7;
		private final int GMT_START = 8;
		private final int GMT_END = 9;

		protected void map(LongWritable key, Text value, Context context) 
				throws IOException ,InterruptedException {
			List<String> fields = new SimpleStringTokenizer(value.toString(), "\t").getAllElements();
			if(fields.size() == 0)
				return;
			
			if(fields.get(COMPANY_NAME).length() == 0)
				return;
				
			/**
			 *  由于一些敏感字会被替换成*号,比如淘宝网,保存在数据库中会变成**网
			 *  考虑最好是能放开这块资料的填写
			 *  否则我们在匹配的时候会跳过这块的数据
			 */
			
			StringBuilder sb= new StringBuilder(fields.get(USER_ID));
			sb.append("\t");
			sb.append(fields.get(BUSINESS_1)).append("\t");
			sb.append(fields.get(BUSINESS_2)).append("\t");
			sb.append(fields.get(POSITION_1)).append("\t");
			sb.append(fields.get(POSITION_2)).append("\t");
			sb.append(fields.get(GMT_START)).append("\t");
			sb.append(fields.get(GMT_END));
			
			context.write(new Text(fields.get(COMPANY_NAME)), new Text(sb.toString()));		
		}
	}
	
	public class SameCompanyReducer extends Reducer<Text, Text, Text, Text> {


		protected void reduce(Text key, Iterable<Text> values, Context context) 
				throws IOException ,InterruptedException {
			
		      for (Text val : values) {
		    	  
		    	  
		      }

		}
		
	}

	public int run(String[] args) throws Exception {
		Job job = new Job(getConf());
		job.setJarByClass(SameCompany.class);
		job.setJobName("SameCompanyStep1");
		job.setNumReduceTasks(5);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		job.setMapperClass(SameCompanyMapper.class);
		job.setReducerClass(SameCompanyReducer.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

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

		boolean success = job.waitForCompletion(true);
		return success ? 0 : 1;
	}	
	
	public static void main(String[] args) throws Exception {
		int ret = ToolRunner.run(new SameCompany(), args);
		System.exit(ret);
	}
	
}
