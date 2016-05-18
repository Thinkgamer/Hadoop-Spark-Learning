package selfSort;

/*
 * 第一列降序，第一列相同时第二列升序
 */

import java.io.DataInput;  
import java.io.DataOutput;  
import java.io.IOException;  
  
import org.apache.hadoop.fs.Path;  
import org.apache.hadoop.io.LongWritable;  
import org.apache.hadoop.io.Text;  
import org.apache.hadoop.io.WritableComparable;  
import org.apache.hadoop.mapreduce.Job;  
import org.apache.hadoop.mapreduce.Mapper;  
import org.apache.hadoop.mapreduce.Reducer;  
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;  
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;  
  
public class selfSort {  
  
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {  
        // TODO Auto-generated method stub  
  
        Job job = new Job();  
        job.setJarByClass(selfSort.class);  
        // 1  
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        // 2  
        job.setMapperClass(Map.class);  
        job.setMapOutputKeyClass(MyK2.class);  
        job.setMapOutputValueClass(LongWritable.class);  
        // 3  
        // 4  
        // 5  
        job.setNumReduceTasks(1);  
        // 6  
        job.setReducerClass(Reduce.class);  
        job.setOutputKeyClass(LongWritable.class);  
        job.setOutputValueClass(LongWritable.class);  
        // 7  
        FileOutputFormat.setOutputPath(job, new Path(args[1]));  
        // 8  
        System.exit(job.waitForCompletion(true)? 0 :1 );  
    }  
public static class Map extends Mapper<Object, Text, MyK2, LongWritable>{  
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException{  
        String line = value.toString();  
        String[] split = line.split("\t");  
        MyK2 my = new MyK2(Long.parseLong(split[0]), Long.parseLong(split[1]));  
        context.write(my, new LongWritable(1));  
    }  
}   
public static class Reduce extends Reducer<MyK2, LongWritable, LongWritable, LongWritable>{  
    public void reduce(MyK2 key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException{  
        context.write(new LongWritable(key.myk2), new LongWritable(key.myv2));  
    }  
}   
  
public static class MyK2 implements WritableComparable<MyK2>{  
  
    public long myk2;  
    public long myv2;  
      
    MyK2(){}  
      
    MyK2(long myk2, long myv2){  
        this.myk2 = myk2;  
        this.myv2 = myv2;  
    }  
      
    @Override  
    public void readFields(DataInput in) throws IOException {  
        // TODO Auto-generated method stub  
        this.myk2 = in.readLong();  
        this.myv2 = in.readLong();  
    }  
  
    @Override  
    public void write(DataOutput out) throws IOException {  
        // TODO Auto-generated method stub  
        out.writeLong(myk2);  
        out.writeLong(myv2);  
    }  
      
    @Override  
    public int compareTo(MyK2  myk2) {  
        // TODO Auto-generated method stub  
        //myk2之差>0 返回-1          <0 返回1 代表 myk2列降序  
        //myk2之差<0 返回-1           >0 返回1 代表 myk2列升序  
        long temp = this.myk2 - myk2.myk2;  
        if(temp>0)  
            return -1;  
        else if(temp<0)  
            return 1;  
        //控制myv2升序  
        return (int)(this.myv2 - myk2.myv2);  
    }  
}  
}  