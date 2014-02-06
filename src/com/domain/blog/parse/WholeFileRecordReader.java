package com.domain.blog.parse;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.RecordReader;

/**
 * @author amalan
 * Record reader used to read complete file instead of usual Text input format
 *
 */
public class WholeFileRecordReader implements RecordReader<NullWritable, Text> {
	private FileSplit fileSplit;
	private Configuration conf;
	private boolean processed = false;

	public WholeFileRecordReader(FileSplit split, Configuration conf) {
		this.fileSplit = split;
		this.conf = conf;
	}

	@Override
	public NullWritable createKey() {
		return NullWritable.get();
	}

	@Override
	public Text createValue() {
		return new Text();
	}

	@Override
	public boolean next(NullWritable key, Text value) throws IOException {
		if (!processed) {
			byte[] contents = new byte[(int) fileSplit.getLength()];
			Path file = fileSplit.getPath();
			FileSystem fs = file.getFileSystem(conf);
			FSDataInputStream in = null;
			try {
				in = fs.open(file);
				IOUtils.readFully(in, contents, 0, contents.length);
				value.set(contents, 0, contents.length);
			} finally {
				IOUtils.closeStream(in);
			}
			processed = true;
			return true;
		}
		return false;
	}

	@Override
	public long getPos() throws IOException {
		return processed ? fileSplit.getLength() : 0;
	}

	@Override
	public void close() throws IOException {

	}

	@Override
	public float getProgress() throws IOException {
		return processed ? 1.0f : 0.0f;
	}
}
