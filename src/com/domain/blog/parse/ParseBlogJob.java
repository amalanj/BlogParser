package com.domain.blog.parse;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * @author amalan
 * The job control program to run the map reduce
 *
 */
public class ParseBlogJob extends Configured implements Tool {

	@Override
	public int run(String[] arg0) throws Exception {

		JobConf job = new JobConf(getConf(), ParseBlogJob.class);
		System.out.println(arg0[0] + " : " + arg0[1]);
		FileInputFormat.addInputPath(job, new Path(arg0[0]));
		FileOutputFormat.setOutputPath(job, new Path(arg0[1]));

		job.set("mapred.map.tasks", "1");
		job.setInputFormat(WholeFileInputFormat.class);
		job.setOutputFormat(CustomFileOutputFormat.class);
		job.setOutputKeyClass(BlogDetails.class);
		job.setOutputValueClass(Text.class);
		job.setMapperClass(ParseBlogMapper.class);
		job.setReducerClass(ParseBlogReducer.class);
		JobClient.runJob(job);
		return 0;
	}

	/**
	 * @param args
	 * @throws Exception
	 */
	public static void main(String[] args) throws Exception {

		int exitCode = ToolRunner.run(new ParseBlogJob(), args);
		System.exit(exitCode);

	}

}
