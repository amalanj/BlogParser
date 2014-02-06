package com.domain.blog.parse;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

/**
 * @author amalan
 * Reducer program to 		
 * 		write the parsed content to the file  
 *
 */
public class ParseBlogReducer extends MapReduceBase implements
		Reducer<BlogDetails, Text, BlogDetails, Text> {

	@Override
	public void reduce(BlogDetails arg0, Iterator<Text> arg1,
			OutputCollector<BlogDetails, Text> arg2, Reporter arg3)
			throws IOException {

		while (arg1.hasNext()) {
			arg2.collect(arg0, arg1.next());
		}

	}

}
