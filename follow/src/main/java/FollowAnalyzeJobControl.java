import java.io.IOException;
import java.util.Map;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;


public class FollowAnalyzeJobControl {

	private static String s = "/group/taobao/taobao/dw/stb/*/sns_follow_statistic";
	private static String so = "/group/taobao-sns/suspect_user/*/follow/follow_action";
	private static String f = "/group/taobao/taobao/dw/stb/*/sns_subscribe";
	private static String fo = "/group/taobao-sns/suspect_user/*/follow/follow_spam";

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		GenericOptionsParser parser = new GenericOptionsParser(conf, args);
		args = parser.getRemainingArgs();
		Options options = new Options();
		options.addOption("s", false, "sns_follow_statistic file path");
		options.addOption("so", false,
				"sns_subscribe analyze result output file path");
		options.addOption("f", false, "sns_subscribe file path");
		options.addOption("fo", false,
				"sns_subscribe suspicion analyze output file path");
		options.addOption("h", false, "help message");
		CommandLineParser cParser = new GnuParser();
		CommandLine commandLine = cParser.parse(options, args, true);
		if (commandLine.hasOption("h")) {
			HelpFormatter formatter = new HelpFormatter();
			formatter.printHelp("FollowAnalyzeJobControl", options);
			return;
		}
		if (commandLine.hasOption("s")) {
			s = commandLine.getOptionValue("s");
		}
		if (commandLine.hasOption("so")) {
			so = commandLine.getOptionValue("so");
		}
		if (commandLine.hasOption("f")) {
			f = commandLine.getOptionValue("f");
		}
		if (commandLine.hasOption("fo")) {
			fo = commandLine.getOptionValue("fo");
		}

		Job job = createFollowActionJob(conf);
		job.waitForCompletion(true);
		job = createFollowAvilabilityJob(conf);
		int res = job.waitForCompletion(true) ? 0 : 1;
		System.exit(res);
	}

	private static Job createFollowActionJob(Configuration conf)
			throws IOException {

		Job job = new Job(conf);
		// Iterator<Map.Entry<String, String>> it = getConf().iterator();
		// while(it.hasNext()){
		// Map.Entry<String, String> entry = it.next();
		// System.out.println(entry.getKey()+"="+entry.getValue());
		// }
		job.setJobName("FollowActionAnalyze");
		job.setJarByClass(FollowAnalyzeJobControl.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		job.setMapperClass(FollowActionMapper.class);
		job.setReducerClass(FollowActionReducer.class);
		job.setCombinerClass(FollowActionMapper.CombinerReduce.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setNumReduceTasks(5);

		Map<String, String> paths = FilePathPartitionUtils.getLastPartitionPath(conf, s, so);
		String input = paths.get("input");
		String output = paths.get("output");

		FileSystem fs = FileSystem.get(conf);
		if (fs.exists(new Path(output))) {
			fs.delete(new Path(output), true);
		}

		System.out.println("input=" + input + ",output=" + output);

		FileInputFormat.addInputPaths(job, input);
		FileOutputFormat.setOutputPath(job, new Path(output));
		return job;
	}

	private static Job createFollowAvilabilityJob(Configuration conf)
			throws IOException {

		Job job = new Job(conf);
		// Iterator<Map.Entry<String, String>> it = getConf().iterator();
		// while(it.hasNext()){
		// Map.Entry<String, String> entry = it.next();
		// System.out.println(entry.getKey()+"="+entry.getValue());
		// }
		job.setJobName("FollowAvailabilityAnalyze");
		job.getConfiguration().set("mapred.child.java.opts","-Xmx512m");
//		job.getConfiguration().set(,"-Xmx512m");

		job.setJarByClass(FollowAnalyzeJobControl.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		job.setMapperClass(FollowAvailabilityMapper.class);
		job.setReducerClass(FollowAvailabilityReducer.class);
		job.setCombinerClass(FollowAvailabilityMapper.CombinerReduce.class);

		job.setMapOutputKeyClass(FollowAvailabilityJoinKey.class);
		job.setMapOutputValueClass(Text.class);
		job.setNumReduceTasks(5);

		Map<String, String> paths = FilePathPartitionUtils.getLastPartitionPath(conf, s, so);
		String followStatisticInput = paths.get("input");
		String suspicionList = paths.get("output");
		paths = FilePathPartitionUtils.getLastPartitionPath(conf, f, fo);
		String followInput = paths.get("input");
		String output = paths.get("output");

		FileSystem fs = FileSystem.get(job.getConfiguration());
		if (fs.exists(new Path(output))) {
			fs.delete(new Path(output), true);
		}

		System.out.println("input=" + followInput + "," + followStatisticInput
				+ "," + suspicionList + ";output=" + output);

		FileStatus[] files = fs.listStatus(new Path(suspicionList));
		for (FileStatus file : files) {
			DistributedCache.addCacheFile(file.getPath().toUri(),
					job.getConfiguration());
		}
		FileInputFormat.addInputPaths(job, followInput);
		FileInputFormat.addInputPaths(job, followStatisticInput);
		FileOutputFormat.setOutputPath(job, new Path(output));
		return job;
	}
}
