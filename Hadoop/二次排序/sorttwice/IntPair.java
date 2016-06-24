package sortTwice;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

//自己定义的InPair类，实现WritableComparator
public class IntPair implements WritableComparable<IntPair>{
	int left;
	int right;
	
	public void set(int left, int right) {
		// TODO Auto-generated method stub
		this.left = left;
		this.right = right;
	}
	public int getLeft() {
		return left;
	}

	public int getRight() {
		return right;
	}
	
	//反序列化，从流中读进二进制转换成IntPair
	@Override
	public void readFields(DataInput in) throws IOException {
		// TODO Auto-generated method stub
		this.left = in.readInt();
		this.right = in.readInt();
	}
	//序列化，将IntPair转换成二进制输出
	@Override
	public void write(DataOutput out) throws IOException {
		// TODO Auto-generated method stub
		out.writeInt(left);
		out.writeInt(right);
	}
	
	/*
	 * 为什么要重写equal方法？
	 * 因为Object的equal方法默认是两个对象的引用的比较，意思就是指向同一内存,地址则相等，否则不相等；
	 * 如果你现在需要利用对象里面的值来判断是否相等，则重载equal方法。
	 */
	@Override
	public boolean equals(Object obj) {
		// TODO Auto-generated method stub
		if(obj == null)
			return false;
		if(this == obj)
			return true;
		if (obj instanceof IntPair){
			IntPair r = (IntPair) obj;
			return r.left == left && r.right==right;
		}
		else{
			return false;
		}
			
	}
	
	/*
	 * 重写equal 的同时为什么必须重写hashcode？ 
	 * hashCode是编译器为不同对象产生的不同整数，根据equal方法的定义：如果两个对象是相等（equal）的，那么两个对象调用 hashCode必须产生相同的整数结果，
	 * 即：equal为true，hashCode必须为true，equal为false，hashCode也必须 为false，所以必须重写hashCode来保证与equal同步。 
	 */
	@Override
	public int hashCode() {
		// TODO Auto-generated method stub
		return left*157 +right;
	}
	
	//实现key的比较
	@Override
	public int compareTo(IntPair o) {
		// TODO Auto-generated method stub
		if(left != o.left)
			return left<o.left? -1:1;
		else if (right != o.right)
			return right<o.right? -1:1;
		else
			return 0;
	}
	
	
}
