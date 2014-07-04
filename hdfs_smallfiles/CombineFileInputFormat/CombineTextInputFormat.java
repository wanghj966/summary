package com.dratio.common.hadoop;

import java.io.IOException;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.CombineFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.CombineFileRecordReader;
import org.apache.hadoop.mapreduce.lib.input.CombineFileSplit;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.util.ReflectionUtils;

/**
 * 可以整合小文件
 * 
 * @param <K>
 *            Key Type
 * @param <V>
 *            Value Type
 * 
 * @author wang.xinming
 */
@SuppressWarnings("unchecked")
public class CombineTextInputFormat<K extends WritableComparable, V extends Writable>
		extends CombineFileInputFormat {
	@Override
	public RecordReader<K, V> createRecordReader(InputSplit genericSplit,
			TaskAttemptContext context) throws IOException {
		return new CombineFileRecordReader((CombineFileSplit) genericSplit,
				context, CombineKeyValueLineRecordReader.class);
	}

	/**
	 * 可整合小文件，并读取键值对的RecordReader
	 * 
	 * @param <K>
	 *            Key Type
	 * @param <V>
	 *            Value Type
	 * @author wang.xinming
	 */
	public static class CombineKeyValueLineRecordReader<K, V> extends
			RecordReader<K, V> {
		/**
		 * 会由TextInputFormat类生成LineRecordReader
		 */
		private RecordReader<K, V> lineRecordReader = null;

		public CombineKeyValueLineRecordReader(CombineFileSplit hsplit,
				TaskAttemptContext context, Integer partition)
				throws IOException, InterruptedException {
			InputFormat inputFormat = (InputFormat) ReflectionUtils
					.newInstance(TextInputFormat.class,
							context.getConfiguration());

			FileSplit fsplit = new FileSplit(hsplit.getPaths()[partition],
					hsplit.getStartOffsets()[partition],
					hsplit.getLengths()[partition], hsplit.getLocations());

			this.lineRecordReader = inputFormat.createRecordReader(fsplit,
					context);
			initialize(fsplit, context);
		}

		@Override
		public void close() throws IOException {
			lineRecordReader.close();
		}

		@Override
		public K getCurrentKey() throws IOException, InterruptedException {
			return lineRecordReader.getCurrentKey();
		}

		@Override
		public V getCurrentValue() throws IOException, InterruptedException {
			return lineRecordReader.getCurrentValue();
		}

		@Override
		public float getProgress() throws IOException, InterruptedException {
			return lineRecordReader.getProgress();
		}

		@Override
		public void initialize(InputSplit split, TaskAttemptContext context)
				throws IOException, InterruptedException {
			if (split instanceof FileSplit) {
				this.lineRecordReader.initialize(split, context);
			}
		}

		@Override
		public boolean nextKeyValue() throws IOException, InterruptedException {
			return lineRecordReader.nextKeyValue();
		}

	}
}
