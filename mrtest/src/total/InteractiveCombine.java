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


public class InteractiveCombine extends Configured implements Tool {
	private static final String PATH_DEFINE = "sns.interactive.paths";
	private static final String WEIGHT_DEFINE = "sns.interactive.weights";
	// Ĭ�Ϸָ���
	private final static String FIELD_SEPERATOR = "\001";
	
	public static class MapClass extends Mapper<BytesWritable, Text, Text, Text> {
		private final static int FROM_USER_ID = 0;
		private final static int TO_USER_ID = 1;
		private final static int FROM_COUNT = 2;
		private final static int TO_COUNT = 3;
		private final static int TYPE_INDEX = 4;
		private static Map<String, Double> weightMap = null;
		private static double weight = 0.0;
		// ����,��ʱ��д����
		private static double totalType = 9.0;
		
		@Override
		public void map(BytesWritable key, Text value, Context context)
				throws IOException ,InterruptedException {
			List<String> fields = new SimpleStringTokenizer(value.toString(), FIELD_SEPERATOR).getAllElements();
			if(NumberUtils.toLong(fields.get(FROM_USER_ID), 0) == 0)
				return;
			if(NumberUtils.toLong(fields.get(TO_USER_ID), 0) == 0)
				return;
			
			double score = 0.0;
			score = NumberUtils.toDouble(fields.get(FROM_COUNT),0)+NumberUtils.toDouble(fields.get(TO_COUNT), 0);
			score = weight*score/20;
			score *= 1/totalType;
			
			context.write(new Text(fields.get(FROM_USER_ID)+"_"+fields.get(TO_USER_ID)), 
					new Text(fields.get(TYPE_INDEX)+FIELD_SEPERATOR+score));
		}
		
		protected void setup(Mapper<BytesWritable,Text,Text,Text>.Context context) 
				throws IOException ,InterruptedException {
			if(weightMap == null){
				String[] paths = context.getConfiguration().getStrings(PATH_DEFINE);
				String[] weights = context.getConfiguration().getStrings(WEIGHT_DEFINE);
				if(paths.length != weights.length)
					return;
				
				weightMap = new HashMap<String, Double>();
				for (int i = 0; i < paths.length; i++) {
					weightMap.put(paths[i], Double.valueOf(weights[i]));
				}
			}
			FileSplit split = (FileSplit) context.getInputSplit();
			String path = split.getPath().toUri().getPath();
			if(path.indexOf("comment") != -1){
				getAndSetWeight("comment");
			}
			else if(path.indexOf("bangwotiao") != -1){
				getAndSetWeight("bangwotiao");
			}
			else if(path.indexOf("jinbi") != -1){
				getAndSetWeight("jinbi");
			}
			else if(path.indexOf("improve_laobao") != -1){
				getAndSetWeight("improve_laobao");
			}
			else if(path.indexOf("improve_phone") != -1){
				getAndSetWeight("improve_phone");
			}
			else if(path.indexOf("privatemsg") != -1){
				getAndSetWeight("privatemsg");
			}
			else if(path.indexOf("replay") != -1){
				getAndSetWeight("replay");
			}
			else if(path.indexOf("improve_aliS") != -1){
				getAndSetWeight("improve_aliS");
			}
			else if(path.indexOf("follow") != -1){
				getAndSetWeight("follow");
			}
		};
		
		private void getAndSetWeight(String keyword){
			for(String key : weightMap.keySet()){
				if(key.indexOf(keyword) != -1){
					weight = weightMap.get(key);
				}
			}
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
				combineType |= (1<<(type-1));
				score += NumberUtils.toDouble(fields.get(SCORE), 0);
			}
			
			String fromUid = "";
			String toUid = "";
			String uids[] = key.toString().split("_");
			fromUid = uids[0];
			toUid = uids[1];
			if(combineType == 0)
				return;
			
			// ����Ŀǰob����汾֮ǰ����double�ڲ�ͬ�İ汾�ϲ�һ�µ�����.
			// ����socre�ȳ���һ������Ȼ����int����
			int mscore = (int)(score * 100000);
			context.write(new Text(),new Text(fromUid+FIELD_SEPERATOR
					+toUid +FIELD_SEPERATOR
					+combineType +FIELD_SEPERATOR
					+mscore));

			context.progress();
		}
	}
	
	@Override
	public int run(String[] args) throws Exception {
		Job job = new Job(getConf());
		job.setJarByClass(InteractiveCombine.class);
		job.setJobName("combine all interactive data ");
		job.setNumReduceTasks(100);
		job.getConfiguration().set("mapred.child.java.opts","-Xmx1024m");
		job.getConfiguration().set("mapred.job.queue.name", "cug-taobao-sns");

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		job.setMapperClass(MapClass.class);
		job.setReducerClass(Reduce.class);

		job.setInputFormatClass(SequenceFileInputFormat.class);
		SequenceFileOutputFormat.setOutputCompressionType(job, SequenceFile.CompressionType.BLOCK);
		job.setOutputFormatClass(SequenceFileOutputFormat.class);

		// ����zeus��ʱ�����������������,�������������Լ�ָ��ʱ��.
		// �滻 �����е�yyyyMMdd
		Calendar cal = Calendar.getInstance();
		cal.set(Calendar.DAY_OF_MONTH, cal.get(Calendar.DAY_OF_MONTH)-1);
		SimpleDateFormat s = new SimpleDateFormat("yyyyMMdd");
		String datepath = s.format(cal.getTime());

		// �ϲ����е�profile��������������
		StringBuilder weights = new StringBuilder();
		StringBuilder paths = new StringBuilder();
		for(int i=0; i<args.length-1; i++){
			String path = args[i].replaceAll("yyyyMMdd", datepath);
			FileInputFormat.addInputPath(job, new Path(path));
			System.out.println(path);
			if(paths.length() > 0)
				paths.append(",");
			paths.append(path);
			i++;
			if(i >= args.length -1){
				System.out.println(" miss weight");
				return 1;
			}
			int weight = NumberUtils.toInt(args[i], -1);
			if(weight == -1){
				System.out.println("weight param error,"+ args[i]);
				return 1;
			}
			if(weights.length() > 0)
				weights.append(",");
			weights.append(args[i]);
		}
		job.getConfiguration().setStrings(PATH_DEFINE, paths.toString());
		job.getConfiguration().setStrings(WEIGHT_DEFINE, weights.toString());
		
		// ���һ�������·��
		String outpath = args[args.length-1].replaceAll("yyyyMMdd", datepath);
		FileOutputFormat.setOutputPath(job, new Path(outpath));

		boolean success = job.waitForCompletion(true);
		return success ? 0 : 1;
	}
	
	public static void main(String[] args) throws Exception {
		int status = ToolRunner.run(new Configuration(), new InteractiveCombine(), args);
		System.exit(status);
	}

}
