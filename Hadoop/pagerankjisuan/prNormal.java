package pagerankjisuan;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class prNormal {
	
	public static class normalMapper extends Mapper<LongWritable,Text,Text,Text>{
		private static Text k = new Text("1");
		public void map(LongWritable key , Text value , Context context) throws IOException, InterruptedException{
//			System.out.println(value.toString());
			context.write(k,value);
		}
	}
	
	public static class normalReducer extends Reducer<Text, Text , Text , Text>{

		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException{
			List<String> list = new ArrayList();
			
			float sum = 0f;
			for(Text text : values){	
				list.add(text.toString());
				
				String[] val = text.toString().split("\t");
				float f = Float.parseFloat(val[1]);
				sum +=f;
			}
			
			for(String line : list){
				String[] vals = line.split("\t");
				Text k = new Text(vals[0]);
                
				float f = Float.parseFloat(vals[1]);
                Text v = new Text(prjob.scaleFloat ( (float)  (f / sum) ));
                context.write(k, v);

//                System.out.println(k + ":" + v);
			}
		}
	}
	
	public static void main(Map<String, String> path) throws IllegalArgumentException, IOException, ClassNotFoundException, InterruptedException, URISyntaxException {
		String input = path.get("input_pr");
        String output = path.get("result");

        hdfsGYT hdfs = new hdfsGYT();
        hdfs.rmr(output);

        Job job = new Job();
        job.setJarByClass(prNormal.class);
		
		//set file input 
		FileInputFormat.addInputPath(job, new Path(input));
		job.setInputFormatClass(TextInputFormat.class);
		
		//set map
		job.setMapperClass(normalMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);

		//set partition
		//set combine
		//set sort
		
		//set  reduce
		job.setReducerClass(normalReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		//set outputpath
		FileOutputFormat.setOutputPath(job, new Path(output));
		job.setOutputFormatClass(TextOutputFormat.class);
		
		//upload job
        job.waitForCompletion(true);
	}
}
