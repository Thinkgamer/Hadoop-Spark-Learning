package pagerankjisuan;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.Map;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.IntWritable.Comparator;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
  
public class prSort {  
	/**
     * @param args
     * @throws IOException 
     * @throws IllegalArgumentException 
     * @throws InterruptedException 
     * @throws ClassNotFoundException 
     */
    public static class myComparator extends Comparator {
        @SuppressWarnings("rawtypes")
        public int compare( WritableComparable a,WritableComparable b){
            return -super.compare(a, b);
        }
        public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
            return -super.compare(b1, s1, l1, b2, s2, l2);
        }
    }
    
    public static class sortMap extends Mapper<Object,Text,FloatWritable,IntWritable>{
        public void map(Object key,Text value,Context context) throws NumberFormatException, IOException, InterruptedException{
            String[] split = value.toString().split("\t");
            context.write(new FloatWritable(Float.parseFloat(split[1])),new IntWritable(Integer.parseInt(split[0])) );
        }
    }
    public static class Reduce extends Reducer<FloatWritable,IntWritable,IntWritable,FloatWritable>{
        public void reduce(FloatWritable key,Iterable<IntWritable>values,Context context) throws IOException, InterruptedException{
            for (IntWritable text : values) {
                context.write( text,key);
            }
        }
    }
  
	public static void main(Map<String, String> path) throws IOException, URISyntaxException, ClassNotFoundException, InterruptedException {
        // TODO Auto-generated method stub  
  
		String input = path.get("result");
		String output = path.get("sort");
		hdfsGYT hdfs = new hdfsGYT();
		hdfs.rmr(output);
		
        Job job = new Job();  
        job.setJarByClass(prSort.class);  
        // 1  
        FileInputFormat.setInputPaths(job, new Path(input) );  
        // 2  
        job.setMapperClass(sortMap.class);  
        job.setMapOutputKeyClass(FloatWritable.class);  
        job.setMapOutputValueClass(IntWritable.class);  
        // 3  
        // 4  自定义排序
        job.setSortComparatorClass( myComparator.class); 
        // 5  
        job.setNumReduceTasks(1);  
        // 6  
        job.setReducerClass(Reduce.class);  
        job.setOutputKeyClass(IntWritable.class);  
        job.setOutputValueClass(FloatWritable.class);  
        // 7  
        FileOutputFormat.setOutputPath(job, new Path(output));  
        // 8  
        System.exit(job.waitForCompletion(true)? 0 :1 );  
    }  
}

