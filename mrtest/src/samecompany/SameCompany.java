package samecompany;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import sameschool.Student;
import util.SimpleStringTokenizer;

/**
 * ͬ��˾��ʱ��,��ҵ�ֶεĺ����е㲻��ȷ. Ŀǰ���ǵ�����,����˾ͬ����ʱ��,��ҵ���������ֹ�˾��.
 * ������һ����˾�� �й���Դ,���Ǵ��µ�����ҵ��, Ȼ������һ����˾Ҳ���й���Դ,�����µ���ʯ����ҵ
 * ���ʱ����Ƽ��ͻ��бȽϴ�Ĳ�ͬ.��Ҫ������ҵ��ȥ
 * @author leibiao
 *
 */
public class SameCompany extends Configured implements Tool {
	private static final int MAX_RECOMMEND = 800;
	
	public static class SameCompanyMapper extends Mapper<LongWritable, Text, Text, Text> {
		private final int OFFSET = 1;
		private final int USER_ID = 1 + OFFSET;
		private final int COMPANY_NAME = 2 + OFFSET;
		private final int BUSINESS_1 = 4 + OFFSET;	// ��ҵ����
		private final int BUSINESS_2 = 5 + OFFSET;	// ��ҵϸ��
		private final int POSITION_1 = 6 + OFFSET;	// ְλ����
		private final int POSITION_2 = 7 + OFFSET;	// ����ְλ
		private final int JOB_GMT_START = 8 + OFFSET;
		private final int JOB_GMT_END = 9 + OFFSET;

		protected void map(LongWritable key, Text value, Context context) 
				throws IOException ,InterruptedException {
			List<String> fields = new SimpleStringTokenizer(value.toString(), "\t").getAllElements();
			if(fields.size() == 0)
				return;
			
			String companyName = fields.get(COMPANY_NAME);
			if(StringUtils.isBlank(companyName))
				return;
				
			/**
			 *  ����һЩ�����ֻᱻ�滻��*��,�����Ա���,���������ݿ��л���**��
			 *  ����������ܷſ�������ϵ���д
			 *  ����������ƥ���ʱ���������������
			 */
			if(companyName.indexOf("*") != -1)
				return;
			
			StringBuilder sb= new StringBuilder(fields.get(USER_ID)).append("\t");
			sb.append(fields.get(BUSINESS_1));
			sb.append(fields.get(BUSINESS_2)).append("\t");
			sb.append(fields.get(POSITION_1)).append("\t");
			sb.append(fields.get(POSITION_2)).append("\t");
			sb.append(fields.get(JOB_GMT_START)).append("\t");
			sb.append(fields.get(JOB_GMT_END));
			
			context.write(new Text(companyName), new Text(sb.toString()));
		}
	}
	
	public static class SameCompanyReducer extends Reducer<Text, Text, Text, Text> {
		private static final int onlyjob = 0;
		private static final int job_business = 1;
		private static final int job_position = 2;
		private static final int job_day = 3;
		private static final int job_pos_bus = 4;
		private static final int job_day_bus = 5;
		private static final int job_day_pos = 6;
		private static final int job_day_pos_bus = 7;
		
		protected void reduce(Text key, Iterable<Text> values, Context context) 
				throws IOException ,InterruptedException {
			List<List<Employee>> jobInfo_list = new ArrayList<List<Employee>>();
			for (int i = 0; i < job_day_pos_bus+1; i++) {
				List<Employee> jobInfo = new ArrayList<Employee>();
				jobInfo_list.add(jobInfo);
			}
			
			Set<String> duplicateCheck = new HashSet<String>();
			Iterator<Text> itor = values.iterator();
			while (itor.hasNext()) {
				Employee e = convertToEmployee(itor.next().toString());
				if(duplicateCheck.contains(e.getUserId()))
					continue;
				duplicateCheck.add(e.getUserId());

				if(e.getType() == Employee.BASIC)
					jobInfo_list.get(onlyjob).add(e);
				else if(e.getType() == Employee.WITHBUSINESS)
					jobInfo_list.get(job_business).add(e);
				else if(e.getType() == Employee.WITHPOSITION)
					jobInfo_list.get(job_position).add(e);
				else if(e.getType() == Employee.WITHDAY)
					jobInfo_list.get(job_day).add(e);
				else if(e.getType() == (Employee.WITHBUSINESS|Employee.WITHPOSITION) )
					jobInfo_list.get(job_pos_bus).add(e);
				else if(e.getType() == (Employee.WITHBUSINESS|Employee.WITHDAY) )
					jobInfo_list.get(job_day_bus).add(e);
				else if(e.getType() == (Employee.WITHPOSITION|Employee.WITHDAY) )
					jobInfo_list.get(job_day_pos).add(e);
				else if(e.getType() == (Employee.WITHPOSITION|Employee.WITHDAY|Employee.WITHBUSINESS) )
					jobInfo_list.get(job_day_pos_bus).add(e);
			}
			
			/**
			 * ����û���κζ�����Ϣ���û�,�Ƽ�������Ϣ���������û�,��Ŀǰ���ǵĲ���.
			 * ����������ʼ�Ƽ�,����ĺ��沿�������Ĳ���
			 * ��������д���κ���Ϣ���û�,�����ȵĲ�����Ѱ�Һ����Ķ�����Ϣƥ����û�.ȫ���ҹ�һ���Ժ�,
			 * �Ű�����Ϣ�����Ƚ����Ƽ�.
			 */
			for(List<Employee> list : jobInfo_list){
				for(Employee e : list){
					matchEmployeeByInfoIntegrity(key, context, jobInfo_list, e, true);
					matchEmployeeByInfoIntegrity(key, context, jobInfo_list, e, false);
				}
			}
		}
		
		private void matchEmployeeByInfoIntegrity(Text key,
				Context context,
				List<List<Employee>> jobInfo_list,
				Employee e, boolean needMatch) throws IOException,InterruptedException {
			Set<String> matchedStudent = new HashSet<String>();
			
			for (int i=jobInfo_list.size()-1; i>=0 ; i--) {
				List<Employee> list = jobInfo_list.get(i);
				for(Employee employee : list){
					if(matchedStudent.size() > MAX_RECOMMEND)
						return;
					if(matchedStudent.contains(employee.getUserId()))
						continue;
					if(e.getUserId().equals(employee.getUserId()))
						continue;
	
					if(!needMatch || findMatchedUser(e, employee)){
						matchedStudent.add(employee.getUserId());
						context.write(new Text(e.getUserId()), new Text(makeOutputValue(key, employee, needMatch)));
					}
				}
			}
		}
	
		/**
		 * �����Ա�������е�type����ԭ��ӵ�е����ԣ������й���ʱ�䣬�в��ŵȵȡ�
		 * ��������˾ͻ������������ϴ��ϱ�ǡ�
		 */
		
		private boolean findMatchedUser(Employee dest,Employee employee) throws IOException {
			if(dest.getType() == Employee.BASIC){
//				employee.setMatchedType(Employee.BASIC_MATCHED);
				return false;
			}
			
			// ʱ��ƥ��
			if( (dest.getType()&Employee.WITHDAY) > 1 ){
				if(dest.getStartDay().equals(employee.getStartDay()))
					employee.setMatchedType(Employee.DAY_MATCHED);
	//			//
	//			SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
	//			long destStart = 0;
	//			long destEnd = 0;
	//			long compareStart = 0;
	//			long compareEnd = 0;
	//			try {
	//				if(dest.getStartDay() != null)
	//					destStart = sdf.parse(dest.getStartDay()).getTime();
	//				if(dest.getEndDay() != null)
	//					destEnd = sdf.parse(dest.getStartDay()).getTime();
	//				if(employee.getStartDay() != null)
	//					compareStart = sdf.parse(employee.getStartDay()).getTime();
	//				if(employee.getEndDay() != null)
	//					compareEnd = sdf.parse(employee.getEndDay()).getTime();
	//			} catch (Exception e) {
	//				// TODO: handle exception
	//			}
	//			//  d1	s1		d2   s2
	//			if(destStart <= compareStart && destEnd >= compareStart && )
			}
			
			// ְλƥ��
			if( (dest.getType()&Employee.WITHPOSITION) == Employee.WITHPOSITION){
				if( (employee.getType()&Employee.WITHPOSITION) == Employee.WITHPOSITION ){
					if(dest.getPositionType().equals(employee.getPositionType()))
						employee.setMatchedType(employee.getMatchedType()|Employee.POSITIONTYPE_MATCHED);
					
					if(StringUtils.isNotBlank(dest.getPosition()) && StringUtils.isNotBlank(employee.getPosition()))
						employee.setMatchedType(employee.getMatchedType()|Employee.POSITION_MATCHED);
				}
			}
			
			// ��ҵƥ��
			if( (dest.getType()&Employee.WITHBUSINESS) == Employee.WITHBUSINESS){
				if( (employee.getType()&Employee.WITHBUSINESS) == Employee.WITHBUSINESS){
					if(dest.getBusinessType().equals(employee.getBusinessType()))
						employee.setMatchedType(employee.getMatchedType()|Employee.BUSINESSTYPE_MATCHED);
					
					if(StringUtils.isNotBlank(dest.getBusinessDetail()) && StringUtils.isNotBlank(employee.getBusinessDetail()) )
						employee.setMatchedType(employee.getMatchedType()|Employee.BUSINESSDETAIL_MATCHED);
				}
			}
			
			if(employee.getMatchedType() > 0)
				return true;
			
			return false;
		}

		private String makeOutputValue(Text key, Employee employee, boolean isMatched) {
			int type = employee.getType();
			if(isMatched)
				type |= Employee.MATCHED;
			return employee.getUserId()+"\t"+key+"\t"+employee.getCombineType();
		}
	
		private Employee convertToEmployee(String string) {
			Employee employee = new Employee();
			SimpleStringTokenizer simpleStringTokenizer = new SimpleStringTokenizer(string, "\t", 7);
			List<String> fields = simpleStringTokenizer.getAllElements();
			
			int type = Employee.BASIC;
			employee.setUserId(fields.get(0));
			if(StringUtils.isNotBlank(fields.get(1))){
				employee.setBusinessType(fields.get(1));
				type |= Employee.WITHBUSINESS;
				if(StringUtils.isNotBlank(fields.get(2)))
						employee.setBusinessDetail(fields.get(2));
			}
			if(StringUtils.isNotBlank(fields.get(3))){
				type |= Employee.WITHPOSITION;
				employee.setPositionType(fields.get(3));
				if(StringUtils.isNotBlank(fields.get(4)))
					employee.setPosition(fields.get(4));
			}
			if(StringUtils.isNotBlank(fields.get(5))){
				type |= Employee.WITHDAY;
				employee.setStartDay(fields.get(5));
				if(StringUtils.isNotBlank(fields.get(6)))
					employee.setEndDay(fields.get(6));
			}
			employee.setType(type);
			
			return employee;
		}
	}
	
	public int run(String[] args) throws Exception {
		Job job = new Job(getConf());
		job.setJarByClass(SameCompany.class);
		job.setJobName("SameCompanyStep1");
		job.getConfiguration().set("mapred.child.java.opts","-Xmx1024m");
		job.getConfiguration().set("io.sort.factor", "100");
		job.getConfiguration().set("io.sort.mb", "512");
		job.setNumReduceTasks(50);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		job.setMapperClass(SameCompanyMapper.class);
		job.setReducerClass(SameCompanyReducer.class);

		job.setInputFormatClass(TextInputFormat.class);
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

		boolean success = job.waitForCompletion(true);
		return success ? 0 : 1;
	}	
	
	public static void main(String[] args) throws Exception {
		int ret = ToolRunner.run(new SameCompany(), args);
		System.exit(ret);
	}
	
}
