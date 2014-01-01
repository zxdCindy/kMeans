import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

/**
 * Review Tuple for TF-IDF
 * @author cindyzhang
 *
 */
public class ReviewTuple implements Writable{
	
	private long termCount;
	private long totalCount;

	public long getTermCount() {
		return termCount;
	}

	public void setTermCount(long termCount) {
		this.termCount = termCount;
	}

	public long getTotalCount() {
		return totalCount;
	}

	public void setTotalCount(long totalCount) {
		this.totalCount = totalCount;
	}

	@Override
	public void readFields(DataInput arg) throws IOException {
		termCount = arg.readLong();
		totalCount = arg.readLong();
	}

	@Override
	public void write(DataOutput arg) throws IOException {
		arg.writeLong(termCount);
		arg.writeLong(totalCount);	
	}

	

}
