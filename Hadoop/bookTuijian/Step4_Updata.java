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
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

/*
 * 是对Step4的优化，分为矩阵相乘和相加，这一步是相乘
 */
public class Step4_Updata {

	public static class Step4_Updata_Map extends Mapper< LongWritable, Text, Text, Text>{

		String filename;
		@Override
		protected void setup(Context context) throws IOException,InterruptedException {
			// TODO Auto-generated method stub
			InputSplit input = context.getInputSplit();
			filename = ((FileSplit) input).getPath().getParent().getName();
			System.out.println("FileName：" +filename);
		}
		
		@Override
		protected void map(LongWritable key, Text value, Context context)	throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			String[] tokens = bookRecommend.DELIMITER.split(value.toString());      //切分
			
			if(filename.equals("Step3_2") ){ //同现矩阵
				String[] v1 = tokens[0].split(":");
				String itemID1 = v1[0];
				String itemID2 = v1[1];
				String num = tokens[1];
				
				Text key1 = new Text(itemID1);
				Text value1 = new Text("A:" + itemID2 +"," +num);
				context.write(key1,value1);
//				System.out.println(key1.toString() + "\t" + value1.toString());
			}else{    //评分矩阵
				String[] v2 = tokens[1].split(":");
				String itemID = tokens[0];
				String userID = v2[0];
				String score = v2[1];
				
				Text key1 = new Text(itemID);
				Text value1 = new Text("B:" + userID + "," + score);
				context.write(key1,value1);
//				System.out.println(key1.toString() + "\t" + value1.toString());
			}
		}
	}

	public static class Step4_Updata_Reduce extends Reducer<Text, Text, Text, Text>{

		@Override
		protected void reduce(Text key, Iterable<Text> values,Context context)	throws IOException, InterruptedException {
			// TODO Auto-generated method stub
//			System.out.println(key.toString()+ ":");
			
			Map mapA = new HashMap();
			Map mapB = new HashMap();
			
			for(Text line : values){
				String val = line.toString();
//				System.out.println(val);
				if(val.startsWith("A")){
					String[] kv = bookRecommend.DELIMITER.split(val.substring(2));
					mapA.put(kv[0], kv[1]); //ItemID, num
//					System.out.println(kv[0] + "\t" + kv[1] + "--------------1");
				}else if(val.startsWith("B")){
					String[] kv = bookRecommend.DELIMITER.split(val.substring(2));
					mapB.put(kv[0], kv[1]); //userID, score
//					System.out.println(kv[0] + "\t" + kv[1] + "--------------2");
				}
			}
			
			double result = 0;
			Iterator iterA = mapA.keySet().iterator();
			while(iterA.hasNext()){
				String mapkA = (String) iterA.next();  //itemID
				int num = Integer.parseInt((String) mapA.get(mapkA));     // num
				Iterator iterB = mapB.keySet().iterator();
				while(iterB.hasNext()){
					String mapkB = (String)iterB.next(); //UserID
					double score = Double.parseDouble((String) mapB.get(mapkB));  //score
					result = num * score; //矩阵乘法结果
					
					Text key2 = new Text(mapkB);
					Text value2 = new Text(mapkA + "," +result);
					context.write(key2,value2); //userID \t  itemID,result
//					System.out.println(key2.toString() + "\t" + value2.toString());
				}
			}
		}
		
	}
	
	public static void run(Map<String, String> path) throws IOException, URISyntaxException, ClassNotFoundException, InterruptedException {
		// TODO Auto-generated method stub
		
		String input_1 = path.get("hdfs_step4_updata_input");
		String input_2 = path.get("hdfs_step4_updata2_input");
		String output = path.get("hdfs_step4_updata_output");
		
		hdfsGYT hdfs = new hdfsGYT();
		hdfs.rmr(output);
		
		Job job = new Job(new Configuration(), "Step4_updata");
		job.setJarByClass(Step4_Updata.class);
		//设置文件输入输出路径
		FileInputFormat.setInputPaths(job, new Path(input_1),new Path(input_2));
		FileOutputFormat.setOutputPath(job, new Path(output));
		
		//设置map和reduce类
		job.setMapperClass(Step4_Updata_Map.class);
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
