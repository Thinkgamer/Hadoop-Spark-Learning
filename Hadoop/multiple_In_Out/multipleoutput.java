package multiple_In_Out;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class multipleoutput {
	
	static String input = "hdfs://127.0.0.1:9000/mr/input";
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
		private MultipleOutputs mos;
		public void setup(Context context){
			mos = new MultipleOutputs(context);
		}
		
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException{
			String k = key.toString();
			for(Text t : values){
				if("中国".equals(k)){
					System.out.println(t.toString());
	                mos.write("china",new Text("中国"), t);
	            }else if("美国".equals(k)){
					System.out.println(t.toString());
	                mos.write("usa",new Text("美国"),t);
	            }else if("中国人".equals(k)){
					System.out.println(t.toString());
	                mos.write("cpeople",new Text("中国人"),t);
	            }
			}
		}
		
		public void cleanup(Context context) throws IOException, InterruptedException{
			mos.close();
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
		
		MultipleOutputs.addNamedOutput(job, "china", TextOutputFormat.class, Text.class, Text.class);
		MultipleOutputs.addNamedOutput(job, "usa", TextOutputFormat.class, Text.class, Text.class);
		MultipleOutputs.addNamedOutput(job, "cpeople", TextOutputFormat.class, Text.class, Text.class);
		
		FileInputFormat.addInputPath(job, new Path(input));
		FileOutputFormat.setOutputPath(job, new Path(output));
		
		System.exit(job.waitForCompletion(true)?0:1);
	}

}
