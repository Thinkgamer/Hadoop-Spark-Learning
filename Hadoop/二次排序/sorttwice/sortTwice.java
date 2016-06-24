package sortTwice;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class sortTwice {

	static String HDFS = "hdfs://127.0.0.1:9000";    //hdfs 路径
	
	//Map类
	public static class ST_Map extends Mapper<LongWritable, Text, IntPair ,IntWritable>{
		private final IntPair intkey = new IntPair();
		private final IntWritable intvalue = new IntWritable();		
		public void map(LongWritable key,Text value,Context context) throws IOException, InterruptedException{
			StringTokenizer itr = new StringTokenizer(value.toString());
			int left =0;
			int right =0;
			while(itr.hasMoreTokens()){
				left = Integer.parseInt(itr.nextToken());
				if(itr.hasMoreTokens())
					right = Integer.parseInt(itr.nextToken());
				intkey.set(left, right);
				intvalue.set(right);
				context.write(intkey, intvalue);
				
			}
		}
	}
	
	//Reducce类
	public static class ST_Reduce extends Reducer<IntPair, IntWritable, Text, IntWritable>{
		private final Text left = new Text();
		private static final Text SEPAPATOR= new Text("================================");
		public void reduce(IntPair key,Iterable<IntWritable>values,Context context) throws IOException, InterruptedException{
			context.write(SEPAPATOR, null);
			left.set(Integer.toString(key.getLeft()));
			for(IntWritable val:values){
				context.write(left, val);
			}
		}
	}
	
	//分区函数类，根据first确定Partition
	public static class MyPartitioner extends Partitioner<IntPair, IntWritable>{
		@Override
		public int getPartition(IntPair key, IntWritable value, int numOfReduce) {
			// TODO Auto-generated method stub
			return Math.abs(key.getLeft()*127) % numOfReduce;
		}
	}
	/**
	 * 在分组比较的时候，只比较原来的key，而不是组合key。
	 */
	public static class MyGroupParator implements RawComparator<IntPair>{

		@Override
		public int compare(IntPair o1 , IntPair o2) {
			// TODO Auto-generated method stub
			int l = o1.getLeft();
			int r = o2.getRight();
			return l == r ? 0:(l<r ?-1:1);
		}
		//一个字节一个字节的比，直到找到一个不相同的字节，然后比这个字节的大小作为两个字节流的大小比较结果。
		@Override
		public int compare(byte[] b1, int l1, int r1, byte[] b2,int l2, int r2) {
			// TODO Auto-generated method stub
            return WritableComparator.compareBytes(b1, l1, Integer.SIZE/8, b2, l2, Integer.SIZE/8);
		}
		
	}
	
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		
		//读取hadoop配置
		Configuration conf =new Configuration(); 
		//初始化作业
		Job job = new Job(conf, "sortTwice");
		job.setJarByClass(sortTwice.class);
		
		//hdfs的输入输出路径
		String input = HDFS + "/mr/mytest/sorttwice/input";
		String output = HDFS + "/mr/mytest/sorttwice/output";
		//1.1指定输入文件的位置
		FileInputFormat.setInputPaths(job, new Path(input));
		//对输入文件进行格式化，设置InputFormat，将输入的数据切分成小的数据块
		job.setInputFormatClass(TextInputFormat.class);
		
		//1.2指定map类
		job.setMapperClass(ST_Map.class);
		//指定map的输出格式
		job.setMapOutputKeyClass(IntPair.class);
		job.setMapOutputValueClass(IntWritable.class);
		
		//1.3指定分区函数
//		job.setPartitionerClass(MyPartitioner.class);
		//指定reducenum个数
		job.setNumReduceTasks(1);
		
		//1.4 TODO 分组，排序
		job.setGroupingComparatorClass(MyGroupParator.class);
		
		//1.5 TODO 归约处理
		
		//2.1
		
		//2.2 指定reduce类
		job.setReducerClass(ST_Reduce.class);
		//设置reduce输入类型
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		//2.3 指定输出文件路径
		FileOutputFormat.setOutputPath(job, new Path(output));
		//提供一个RecordReader实现数据的输出
		job.setOutputFormatClass(TextOutputFormat.class);
		
		//提交作业
		System.exit(job.waitForCompletion(true)? 0:1);
	}
}
