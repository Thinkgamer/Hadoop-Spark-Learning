package multiple_In_Out;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class multipleinout {

	static String input1 = "hdfs://127.0.0.1:9000/mr/input1";
	static String input2 = "hdfs://127.0.0.1:9000/mr/input2";
	static String output = "hdfs://127.0.0.1:9000/mr/output";

	public static class Map extends Mapper<LongWritable, Text , Text, Text>{
		private static Text k = new Text();
		private static Text v = new Text();
		
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
			String[] list = value.toString().split(",");
			k.set(list[0]);
			v.set(list[1]);
			context.write(k, v);
		}		
	}
	
	public static class Reduce extends Reducer<Text , Text, Text, Text>{
		
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException{
			for (Text text : values) {
				context.write(key, text);
			}
		}

	}
	/**
	 * @param args
	 * @throws IOException 
	 * @throws InterruptedException 
	 * @throws ClassNotFoundException 
	 */
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		// TODO Auto-generated method stub
		Job job = new Job();
		job.setJarByClass(multipleoutput.class);
		
		job.setMapperClass(Map.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		
		job.setReducerClass(Reduce.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		MultipleInputs.addInputPath(job, new Path(input1), TextInputFormat.class, Map.class);
		MultipleInputs.addInputPath(job, new Path(input2), TextInputFormat.class, Map.class);
		
		FileOutputFormat.setOutputPath(job, new Path(output));
		
		System.exit(job.waitForCompletion(true)?0:1);
	}

}
