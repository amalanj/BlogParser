package com.domain.blog.parse;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

/**
 * @author amalan
 * Custom Output key having Title and Date for folder name and file name
 *
 */
public class BlogDetails implements WritableComparable<BlogDetails> {

	private String title;

	private String date;

	public void set(String title, String date) {

		this.title = title;
		this.date = date;

	}

	public String getTitle() {
		return title;
	}

	public String getDate() {
		return date;
	}

	@Override
	public void readFields(DataInput in) throws IOException {

		this.title = in.readUTF();
		this.date = in.readUTF();

	}

	@Override
	public void write(DataOutput out) throws IOException {

		out.writeUTF(this.title);
		out.writeUTF(this.date);

	}

	@Override
	public int compareTo(BlogDetails blogDetails) {

		return 0;
	}

}
