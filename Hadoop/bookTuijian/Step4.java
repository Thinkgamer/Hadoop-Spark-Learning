package bookTuijian;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
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

public class Step4 {
	
	public static class Step4_Map extends Mapper<LongWritable, Text, Text, Text>{

		String filename;
		static Text k1 =new Text();
		static Text value1 = new Text();
		
		private final static Map<Integer, List<Coocurence>> coocurenceMatrix = 	new HashMap<Integer, List<Coocurence>>();
		
		@Override
		protected void setup(Context context) throws IOException,InterruptedException {
			// TODO Auto-generated method stub
			InputSplit inputsplit = context.getInputSplit();
			filename = ((FileSplit) inputsplit).getPath().getName();
			System.out.println("Step4 Filename : " + filename);
		}
		
		@Override
		protected void map(LongWritable key, Text value, Context context)throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			String[] arrs = bookRecommend.DELIMITER.split(value.toString());
//			System.out.println(value.toString() + "==================");

			String [] v1 = arrs[0].split(":");
			String [] v2 = arrs[1].split(":");
			
			if(v1.length>1)  //数据来自同现矩阵
			{
//				System.out.println(value.toString()+"++++++++++++++++++++++++++==");
				int itemID1 = Integer.parseInt(v1[0]);
				int itemID2 = Integer.parseInt(v1[1]);
				int num = Integer.parseInt(arrs[1]);
				
				List list = null;
				if(!coocurenceMatrix.containsKey(itemID1)){
					list = new ArrayList();
				}else{
					list = coocurenceMatrix.get(itemID1 );
				}
				list.add(new Coocurence(itemID1, itemID2, num) );
				coocurenceMatrix.put(itemID1, list);
			}
			if(v2.length>1) //数据来自评分矩阵
			{
				System.out.println(value.toString()+"-------------------------------");
				int itemID = Integer.parseInt(arrs[0]);
				String userID = v2[0];
				double score = Float.parseFloat(v2[1]);
				k1.set(userID);
				for(Coocurence co : coocurenceMatrix.get(itemID))
				{
					value1.set(co.getItemID2() + "," + score * co.getNum());
					context.write(k1, value1);
					//itemID1, itemID2 +"," + score * num
				}
			}
		}
	}
	
	public static class Step4_Reduce extends Reducer<Text, Text, Text,Text>{

		private static Text value2 = new Text();
		
		@Override
		protected void reduce(Text key, Iterable<Text> values,Context  context)	throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			Map<String,Double> result  = new HashMap<String, Double>();
			for (Text text : values) {
				String[] arrs =text.toString().split(",");
				if (result.containsKey(arrs[0]))
				{
					result.put(arrs[0], result.get(arrs[0]) + Double.parseDouble(arrs[1]));
				}else
				{
					result.put(arrs[0], Double.parseDouble(arrs[1]));
				}
			}
			Iterator iter = result.keySet().iterator();
			while(iter.hasNext())
			{
				String itemID = (String) iter.next();
				double score = result.get(itemID);
				value2.set(itemID + "," + score);
				context.write(key, value2);
			}
		}
	}

	public static void run(Map<String, String> path) throws IOException, URISyntaxException, ClassNotFoundException, InterruptedException {
		// TODO Auto-generated method stub
		String input_1 = path.get("hdfs_step4_input_1");
		String input_2 = path.get("hdfs_step4_input_2");
		String output = path.get("hdfs_step4_output");
		
		hdfsGYT hdfs = new hdfsGYT();
		hdfs.rmr(output);
		
		Job job = new Job( new Configuration(), "Step4");
		job.setJarByClass(Step4.class);
		
		//设置文件路径
		FileInputFormat.setInputPaths(job, new Path(input_2),new Path(input_1));
		FileOutputFormat.setOutputPath(job, new Path(output));

		//设置Map和Reduce类
		job.setMapperClass(Step4_Map.class);
		job.setReducerClass(Step4_Reduce.class);
			
		//设置map的输入输出格式
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
				
		//设置reduce的输入输出格式
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
				
		//设置文件
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
				
		//提交作业
		job.waitForCompletion(true);
		}
	}

class Coocurence{
	private int itemID1;
	private int itemID2;
	private int num;
	
	public Coocurence(int itemID1, int itemID2, int num){
		this.itemID1 = itemID1;
		this.itemID2 = itemID2;
		this.num = num;
	}
	
	public int getItemID1() {
		return itemID1;
	}
	public void setItemID1(int itemID1) {
		this.itemID1 = itemID1;
	}
	public int getItemID2() {
		return itemID2;
	}
	public void setItemID2(int itemID2) {
		this.itemID2 = itemID2;
	}
	public int getNum() {
		return num;
	}
	public void setNum(int num) {
		this.num = num;
	}	
}