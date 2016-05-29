package sort_twice;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class Intpair implements WritableComparable<Intpair>{
	int first;
	int second;
	
	public void set(int first,int second){
		this.first = first;
		this.second = second;
	}
	
	public int getFirst() {
		// TODO Auto-generated method stub
		return first;
	}
	
	public int getSecond() {
		// TODO Auto-generated method stub
		return second;
	}

	//序列化，从流中读进二进制转换成InPair
	@Override
	public void readFields(DataInput in) throws IOException {
		// TODO Auto-generated method stub
		first = in.readInt();
		second = in.readInt();
	}

	//范序列化，将Intpair转换成二进制输出
	@Override
	public void write(DataOutput out) throws IOException {
		// TODO Auto-generated method stub
		out.writeInt(first);
		out.writeInt(second);
	}

	//先按照first比较再按照second比较
	@Override
	public int compareTo(Intpair o) {
		// TODO Auto-generated method stub
		if(first != o.first){
			return first < o.first?-1:1;
		}else if(second !=o.second){
			return second < o.second?-1:1;
		}else{
			return 0;
		}
	}
	
	@Override
    //The hashCode() method is used by the HashPartitioner (the default partitioner in MapReduce)
    public int hashCode()
    {
		return first+"".hashCode() + second+"".hashCode();
    }
	
    @Override
    public boolean equals(Object right)
    {
    	 if (right instanceof Intpair) {
    	      Intpair r = (Intpair) right;
    	      return r.first == first && r.second == second;
    	    } else {
    	      return false;
    	    }
    }
}
