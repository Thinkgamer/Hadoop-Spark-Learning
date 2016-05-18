package sort_twice;

import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.io.WritableComparator;

public class groupingComparator implements RawComparator<Intpair> {
	@Override
	  public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
	    return WritableComparator.compareBytes(b1, s1, Integer.SIZE/8, b2, s2, Integer.SIZE/8);
	  }
	@Override
	public int compare(Intpair o1, Intpair o2) {
		// TODO Auto-generated method stub
		int first1 = o1.getFirst();
	    int first2 = o2.getFirst();
	    return first1 - first2;
	}
}
