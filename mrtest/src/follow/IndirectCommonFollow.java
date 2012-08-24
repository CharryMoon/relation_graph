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
 * ȡBI����õ��Ĺ�ͬ��ע������,Ȼ��ת��������ʹ�õĸ�ʽ.��������
 * ��ȫ��ת��,�޿����������޵�����
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
	// Ĭ�Ϸָ���
	private final static String FIELD_SEPERATOR = "\001";
	private final static String CONTENT_SEPERATOR = "\002";
	// ��ע���ٽ�ֵ,����������ֵ,��ʾ���ŶȺܴ�,С�����󽵵�������Ŷ�
	private final static int trust_num_valve = 10;
	
	public static class IndirectCommonFollowMapper extends Mapper<BytesWritable, Text, Text, Text> {

		protected void map(BytesWritable key, Text value, Context context)
				throws IOException ,InterruptedException {
			List<String> fields = new SimpleStringTokenizer(value.toString(), FIELD_SEPERATOR, FIELD_MAX).getAllElements();
			if(fields.size()  < FIELD_MAX){
				context.setStatus(fields.get(0)+" "+fields.get(1)+" " +fields.get(2)+" " +fields.get(3)+" "+fields.get(4));
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

			double degreeWeight = NumberUtils.toDouble(context.getConfiguration().get("degreeWeight"), Wt);
			double distributeParam = (1-NumberUtils.toDouble(context.getConfiguration().get("distributeParam"), Wk));
			score = Math.sqrt(sonWeight)*degreeWeight*distributeParam/20;
			// ����Ŀǰob����汾֮ǰ����double�ڲ�ͬ�İ汾�ϲ�һ�µ�����.
			// ����socre�ȳ���һ������Ȼ����int����
			int mscore = (int)(score * 100000);

			StringBuilder sb = new StringBuilder();
			sb.append(fields.get(USERID)).append(FIELD_SEPERATOR);
			sb.append(TYPE).append(FIELD_SEPERATOR);
			sb.append(fields.get(TARGETID)).append(FIELD_SEPERATOR);
			sb.append(count).append(FIELD_SEPERATOR);
			sb.append(mscore).append(FIELD_SEPERATOR);
			sb.append(fields.get(FRIEND_IDS).replaceAll(",", CONTENT_SEPERATOR));

			context.write(new Text(), new Text(sb.toString()));
		}
	}
	
	public int run(String[] args) throws Exception {
		Job job = new Job(getConf());
		job.setJarByClass(IndirectCommonFollow.class);
		job.setJobName("indirect common follow");
		job.setNumReduceTasks(0);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		job.setMapperClass(IndirectCommonFollowMapper.class);

		job.setInputFormatClass(SequenceFileInputFormat.class);
		SequenceFileOutputFormat.setOutputCompressionType(job, SequenceFile.CompressionType.BLOCK);
		job.setOutputFormatClass(SequenceFileOutputFormat.class);

		// ����zeus��ʱ�����������������,�������������Լ�ָ��ʱ��.
		// �滻 �����е�yyyyMMdd
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
		int ret = ToolRunner.run(new IndirectCommonFollow(), args);
		System.exit(ret);
	}
	
}
