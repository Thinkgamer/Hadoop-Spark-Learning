package sort_twice;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Partitioner;

public class myPartition extends Partitioner<Intpair,IntWritable> {

	@Override
	public int getPartition(Intpair key, IntWritable value, int numOfReducer) {
		// TODO Auto-generated method stub
		
		return Math.abs(key.getFirst() * 127) % numOfReducer ;
	}

}
