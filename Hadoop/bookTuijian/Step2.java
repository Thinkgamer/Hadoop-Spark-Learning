package bookTuijian;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class Step2 {
	
	//Map
	public static class Step2_Map extends Mapper<LongWritable, Text, Text, LongWritable>{

		String filename;
		static Text k1  = new Text();
		static LongWritable v1 = new LongWritable(1);
		@Override
		protected void setup(Context context) throws IOException,InterruptedException {
			// TODO Auto-generated method stub
			InputSplit inputsplit = context.getInputSplit();
			filename = ((FileSplit) inputsplit).getPath().getName();
			System.out.println("Step2 FileNme:" + filename);
		}
		

		@Override
		protected void map(LongWritable key, Text value, Context context)throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			String[] arrs = bookRecommend.DELIMITER.split(value.toString());
			for (int i =1; i < arrs.length; i++ )
			{
				String itemID = arrs[i].split(":")[0];
				for( int j=1; j< arrs.length; j++)
				{
					String itemID2 = arrs[j].split(":")[0];
					k1.set(itemID+":" + itemID2);
					context.write(k1,v1);
				}
			}
		}
	}

	public static class Step2_Reduce extends Reducer<Text , LongWritable, Text, LongWritable>{

		static LongWritable v2 = new LongWritable();
		@Override
		protected void reduce(Text key, Iterable<LongWritable> values,Context context)throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			int num = 0;
			for (LongWritable text : values) {
				num += text.get();
			}
			v2.set(num);
			context.write(key,v2);
		}
		
	} 
	
	
	public static void run(Map<String, String> path) throws IOException, URISyntaxException, ClassNotFoundException, InterruptedException {
		// TODO Auto-generated method stub
		String input_path = path.get("hdfs_step2_input"); 
		String output_path = path.get("hdfs_step2_output");
		
		hdfsGYT hdfs = new hdfsGYT();
		hdfs.rmr(output_path);
		
		Job job = new Job(new Configuration(), "Step2");
		job.setJarByClass(Step2.class);
		
		//设置文件路径
		FileInputFormat.setInputPaths(job, new Path(input_path));
		FileOutputFormat.setOutputPath(job, new Path(output_path));
		
	    //设置Map和Reduce类
		job.setMapperClass(Step2_Map.class);
		job.setReducerClass(Step2_Reduce.class);
		
		//设置map的输出格式
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(LongWritable.class);
		
		//设置reduce的输出格式
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(LongWritable.class);
				
		//设置文件
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
				
		//提交作业
		job.waitForCompletion(true);
	}

}
