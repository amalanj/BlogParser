package com.domain.blog.parse;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.markdown4j.Markdown4jProcessor;

/**
 * @author amalan
 * Mapper program to 
 * 		parse the input files
 * 		process markdown notations
 * 		collects respective key and value  
 *
 */
public class ParseBlogMapper extends MapReduceBase implements
		Mapper<NullWritable, Text, BlogDetails, Text> {

	@Override
	public void map(NullWritable arg0, Text arg1,
			OutputCollector<BlogDetails, Text> arg2, Reporter arg3)
			throws IOException {

		BlogDetails blogDetails = new BlogDetails();

		int TITLE_INDEX = 2;
		int DATE_INDEX = 4;
		int PUBLISHED_INDEX = 8;
		int MARKDOWN_INDEX = 10;

		String regex = "(Title:\\s+)(.*)[^\\n]*\\n+(Date:\\s+)(\\d{2}-?\\d{2}-?\\d{4})[^\\n]*\\n+(Author:\\s+)(.*)[^\\n]*\\n+(Published:\\s+)(\\w+\\s+)[^\\n]*\\n+(--)[^\\n]*\\n+(.*)";
		Pattern pattern = Pattern.compile(regex);
		Matcher matcher = pattern.matcher(arg1.toString());

		Text value = new Text();
		if (matcher.find()) {

			blogDetails.set(matcher.group(TITLE_INDEX),
					matcher.group(DATE_INDEX));
			value.set(new Markdown4jProcessor().process(matcher
					.group(MARKDOWN_INDEX)));
			if ("true".equalsIgnoreCase(matcher.group(PUBLISHED_INDEX).trim()))
				arg2.collect(blogDetails, value);
		}

	}

}
