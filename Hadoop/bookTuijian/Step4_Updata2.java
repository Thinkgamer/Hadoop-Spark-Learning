package bookTuijian;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
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


public class Step4_Updata2 {

	public static class Step4_Updata2_Map extends Mapper<LongWritable, Text, Text, Text>{

		@Override
		protected void map(LongWritable key, Text value, Context context)throws IOException, InterruptedException {
			// TODO Auto-generated method stub
		String[] tokens = bookRecommend.DELIMITER.split(value.toString());
		Text key1 = new Text(tokens[0]);//userID 
		Text value1 = new Text(tokens[1] + "," + tokens[2]);
		context.write(key1, value1);    //itemID,result
		}
		
	}
	
	public static class Step4_Updata_Reduce extends Reducer< Text, Text, Text, Text>{

		@Override
		protected void reduce(Text key, Iterable<Text> values, Context context)throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			Map map = new HashMap();
			
			for(Text line: values){
				System.out.println(line.toString());
				String[] tokens = bookRecommend.DELIMITER.split(line.toString());
				String itemID = tokens[0];
				Double result = Double.parseDouble(tokens[1]);
				
				if(map.containsKey(itemID)){
					map.put(itemID, Double.parseDouble(map.get(itemID).toString()) + result);//矩阵乘法求和计算
				}else{
					map.put(itemID, result);
				}
			}
			Iterator iter = map.keySet().iterator();
            while (iter.hasNext()) {
                String itemID = (String) iter.next();
                double score = (double) map.get(itemID);
                Text v = new Text(itemID + "," + score);
                context.write(key, v);
            }
		}
		
	}
	
	public static void run(Map<String, String> path) throws IOException, URISyntaxException, ClassNotFoundException, InterruptedException {
		// TODO Auto-generated method stub
		String input = path.get("hdfs_step4_updata2_input");
		String output = path.get("hdfs_step4_updata2_output");
		
		hdfsGYT hdfs = new hdfsGYT();
		hdfs.rmr(output);
		
		Job job = new Job(new Configuration(), "Step4_Updata2");
		job.setJarByClass(Step4_Updata2.class);
		
		FileInputFormat.addInputPath(job, new Path(input));
		FileOutputFormat.setOutputPath(job, new Path(output));
		
		//设置map和reduce类
		job.setMapperClass(Step4_Updata2_Map.class);
		job.setReducerClass(Step4_Updata_Reduce.class);
		
		//设置Map输出
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
				
		//设置Reduce输出
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
			
		//设置文件输入输出
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
				
		job.waitForCompletion(true);
	}

}
