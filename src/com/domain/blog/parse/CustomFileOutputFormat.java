package com.domain.blog.parse;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.lib.MultipleTextOutputFormat;

/**
 * @author amalan
 * Custom file output format to generate file names dynamically
 *
 */
public class CustomFileOutputFormat extends
		MultipleTextOutputFormat<BlogDetails, Text> {

	@Override
	protected BlogDetails generateActualKey(BlogDetails key, Text value) {

		return null;
	}

	@Override
	protected String generateFileNameForKeyValue(BlogDetails key, Text value,
			String leaf) {

		String path = "posts" + System.getProperty("file.separator")
				+ key.getDate() + System.getProperty("file.separator");
		leaf = key.getTitle().replace(".markdown", ".html");
		return new Path(path, leaf).toString();
	}

}
