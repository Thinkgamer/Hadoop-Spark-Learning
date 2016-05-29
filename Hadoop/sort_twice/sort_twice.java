package sort_twice;

import java.io.IOException;
import java.util.Date;
import java.util.StringTokenizer;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class sort_twice {

	/**
	 * @param args
	 * @throws IOException 
	 * @throws InterruptedException 
	 * @throws ClassNotFoundException 
	 */
	public static class Map extends Mapper<Object,Text,Intpair,IntWritable>{
		private final  Intpair intkey = new Intpair();
		private final  IntWritable intvalue = new IntWritable();
		
		public void map(Object key, Text value,Context context) throws IOException, InterruptedException{
			StringTokenizer token = new StringTokenizer(value.toString());
			int left = 0;
			int right = 0;
			while (token.hasMoreElements()){
				left = Integer.parseInt( token.nextToken());
				if(token.hasMoreTokens())
					right = Integer.parseInt(token.nextToken());
				intkey.set(left,right);
				intvalue.set(right);
				context.write(intkey, intvalue);
			}
		}
		
	}

	public static class Reduce extends Reducer<Intpair,IntWritable,Text,IntWritable>{
		private final Text left = new Text();
		private final Text SEPAPATOR= new Text("------------^^我们是同一个分组的^^-----------");
		public void reduce(Intpair key,Iterable<IntWritable> values,Context context) throws IOException, InterruptedException{
			left.set(Integer.toString(key.getFirst()));
			context.write(SEPAPATOR, null);
			for(IntWritable val:values){
				context.write(left, val);
			}
		}
	}

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		// TODO Auto-generated method stub
		Job job = new Job();
		job.setJarByClass(sort_twice.class);
		
		//1 指定输入文件路径
		FileInputFormat.addInputPath(job, new Path(args[0]));
		job.setInputFormatClass(TextInputFormat.class);
		
		//2 设置Map相关
		job.setMapperClass(Map.class);
		job.setMapOutputKeyClass(Intpair.class);
		job.setMapOutputValueClass(IntWritable.class);
		
		//3 设置分区和reducer数目
		job.setPartitionerClass(myPartition.class);
		
		//4 重写分组函数
		job.setGroupingComparatorClass(groupingComparator.class);
		
		//5 归约处理
		//6 指定reducer类
		job.setReducerClass(Reduce.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		//7设置输出路径
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		//8 提交任务
		int result = job.waitForCompletion(true)? 0 : 1;    //任务开始
		

		//输出任务相关的信息
		Date start = new Date();
		Date end = new Date();
		float time = 	(float)(end.getTime()-start.getTime());
		
		System.out.println("Job ID:"+job.getJobID());
		System.out.println("Job Name:"+job.getJobName());
		System.out.println("Job StartTime:"+start);
		System.out.println("Job EndTime:" + end);
		System.out.println("Job 经历的时间：" + time);
		System.out.println("Job 是否成功:"+job.isSuccessful());
		System.out.println(result);
	}	
}