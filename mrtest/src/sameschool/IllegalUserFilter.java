package sameschool;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import samecompany.SameCompany.SameCompanyMapper;
import samecompany.SameCompany.SameCompanyReducer;
import util.SimpleStringTokenizer;

public class IllegalUserFilter extends Configured implements Tool {
	private static String source_filename ;
	private static List<String> filter_filename ;
	private static final int fromsource = 1;
	private static final int fromfilter = 2;
	private static int fromtype = 1;

	public static class IllegalFilterMapper extends Mapper<LongWritable, Text, Text, Text> {

		private static final int USER_ID = 0;	// 用户id
		private static final int SCHOOL_ID = 1;	// 学校id
		private static final int ENTER_DAY = 2;	// 入学时间
		private static final int CAREER = 3;	// (1, "高中"),(2,"大专"), (3, "大学"), (4, * "硕士"), (5, "博士");

		protected void map(LongWritable key, Text value, Context context) 
				throws java.io.IOException ,InterruptedException {
			if(fromtype == fromsource){
				List<String> fields = new SimpleStringTokenizer(value.toString(), "\t").getAllElements();
				context.write(new Text(fields.get(USER_ID)), new Text(fromsource+"\t"+value.toString()));
			}
			else{
				List<String> fields = new SimpleStringTokenizer(value.toString(), "\001").getAllElements();
				context.write(new Text(fields.get(0)), new Text(fromfilter+"\t"));
//				System.out.println("key: "+fields.get(0)+"  value: "+value);
			}
		};
		
		protected void setup(Context context) 
				throws IOException ,InterruptedException {
			FileSplit split = (FileSplit) context.getInputSplit();
			String path = split.getPath().toUri().getPath();
			System.out.println(path);
			if(source_filename == null){
				source_filename = context.getConfiguration().get("source_filename");
			}
			if(filter_filename == null){
				filter_filename = new ArrayList<String>();
				for(String str : context.getConfiguration().getStrings("filter_filename")){
					filter_filename.add(str);
				}
			}
			
			if (path.indexOf(source_filename) != -1) {
				fromtype = fromsource;
				return;
			}
			
			for(String f : filter_filename){
				if(path.indexOf(f) != -1){
					fromtype = fromfilter;
					return;
				}
			}
			System.out.println("no match any path, "+path);
		};
	}

	public static class IllegalFilterReducer extends Reducer<Text, Text, Text, Text> {
		protected void reduce(Text key, Iterable<Text> values, Context context) 
				throws IOException ,InterruptedException {
			Iterator<Text> itor = values.iterator();
			boolean isFilter = false;
			List<String> sourceContent = new ArrayList<String>();
			while (itor.hasNext()) {
				String value = itor.next().toString();
//				System.out.println("key: "+key.toString()+",  value: "+value);
				int index = value.indexOf("\t");
				if(index == -1)
					continue;
				
				int type = Integer.valueOf(value.substring(0,index));
				if(type == fromsource)
					sourceContent.add(value.substring(index+1));
				else if(type == fromfilter){
					 isFilter = true;
//					 System.out.println("filter user:"+value);
					 return;
				}
				
				for(String str : sourceContent){
					context.write(new Text(), new Text(str));
				}
			}
			
		}
		
	}
	
	public int run(String[] args) throws Exception {
		Job job = new Job(getConf());
		job.setJarByClass(IllegalUserFilter.class);
		job.setJobName("IllegalUserFilter");
		job.getConfiguration().set("mapred.job.queue.name", "cug-taobao-sns");
		job.getConfiguration().set("mapred.child.java.opts","-Xmx1024m");
		job.getConfiguration().set("io.sort.factor", "100");
		job.getConfiguration().set("io.sort.mb", "512");
		job.setNumReduceTasks(5);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		job.setMapperClass(IllegalFilterMapper.class);
		job.setReducerClass(IllegalFilterReducer.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		// 替换 参数中的yyyyMMdd
		Calendar cal = Calendar.getInstance();
		cal.set(Calendar.DAY_OF_MONTH, cal.get(Calendar.DAY_OF_MONTH)-1);
		SimpleDateFormat s = new SimpleDateFormat("yyyyMMdd");
		String datepath = s.format(cal.getTime());
		// input source
		String spath = args[0].replaceAll("yyyyMMdd", datepath);
		System.out.println(spath);
		FileInputFormat.addInputPath(job, new Path(spath));
		job.getConfiguration().set("source_filename", spath);
		// filter source
		filter_filename = new ArrayList<String>();
		String fpaths = args[1].replaceAll("yyyyMMdd", datepath);
		job.getConfiguration().setStrings("filter_filename", fpaths);
		for(String fpath : fpaths.split(",")){
			FileInputFormat.addInputPath(job, new Path(fpath));
			System.out.println(fpath);
		}
		String outpath = args[2].replaceAll("yyyyMMdd", datepath);
		FileOutputFormat.setOutputPath(job, new Path(outpath));

		boolean success = job.waitForCompletion(true);
		return success ? 0 : 1;
	}	

	public static void main(String[] args) throws Exception {
		int ret = ToolRunner.run(new IllegalUserFilter(), args);
		System.exit(ret);
	}
}
