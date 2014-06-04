package org.hedera.io.input;

import java.io.IOException;

import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;


public class WikiRevisionETLReader<KEYIN, VALUEIN, REVISIONCONTENT> 
		extends RecordReader<KEYIN, VALUEIN> {

	/** The acknowledgement flag when invoking one internal consuming method.
	 * There are three states can return:
	 * - PASSED_TO_NEXT_TAG: the consumer succeeds and now passed the next tag
	 * - EOF: the consumer doesn't encounter the desired tag and it reaches
	 * the file EOF byte
	 * - SKIPPED: The consumter doesn't reach the desired tag yet, but
	 * it will skip to the end of the page
	 * - FAILED: The consumer fails due to internal errors 
	 */
	protected static enum Ack {
		PASSED_TO_NEXT_TAG,
		EOF,
		SKIPPED,
		FAILED
	}
	
	@Override
	public KEYIN getCurrentKey() throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public VALUEIN getCurrentValue() throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public float getProgress() throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public void initialize(InputSplit inputSplit, TaskAttemptContext tac)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public boolean nextKeyValue() throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		return false;
	}
	
	/**
	 * Consume all the tags from <page> till the first <revision>
	 * @return true when reaching <revision>, false when EOF
	 */
	protected Ack readToPageHeader() throws IOException {
		throw new UnsupportedOperationException();
	}
	
	/**
	 * Inside the <revision></revision> block, read till the end
	 */
	protected Ack readToNextRevision() throws IOException {
		throw new UnsupportedOperationException();
	}
	
	/**
	 * Outside the <revision></revision> block
	 */
	protected boolean hasNextRevision() {
		throw new UnsupportedOperationException();
	}
	
	
	
	@Override
	public void close() throws IOException {
		// TODO Auto-generated method stub
		
	}

}
