package pagerankjisuan;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
public class prJisuan {
	
	public static class prJisuanMapper extends Mapper <LongWritable,Text,Text,Text>{
		
		private String flag; //tmp1  or  result
		private static int nums = 100;  //页面数
		private static Text k =new Text();
		private static Text v =new Text();
		
		protected void setup(Context context){
			FileSplit split = (FileSplit) context.getInputSplit();
			flag = split.getPath().getParent().getName();  //判断读的数据集
		}
		
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
		
		if(flag.endsWith("tmp1")){
			String[] tokens = value.toString().split("\t");
			String row =tokens[0];
			String[] vals = tokens[1].split(",");//转置矩阵
			for (int i =0;i<vals.length;i++){
				k = new Text(String.valueOf(i+1) );
				v = new Text(String.valueOf("A:" + (row) + "," + vals[i]) );
				context.write(k, v);
			}
		}else if(flag.equals("pr")){
			String[] tokens = value.toString().split("\t");
			for(int i=1;i<=nums;i++){
				k = new Text(String.valueOf(i));
				v = new Text("B:" + tokens[0]+ "," + tokens[1] );
				context.write(k,v);
			}
		}
		
		}
	}
	
	public static class prJisuanReducer extends Reducer<Text, Text, Text, Text>{
		
		public void reduce(Text key, Iterable<Text> values,Context context) throws IOException, InterruptedException{
			Map<Integer, Float> mapA = new HashMap<Integer,Float>();
			Map<Integer, Float> mapB = new HashMap<Integer,Float>();
			float pr = 0f;
			for (Text val : values) {
//				System.out.println(val.toString());
				String value = val.toString();
				if(value.startsWith("A") ){
					String[] tokenA  = value.split(":")[1].split(",");
					mapA.put(Integer.parseInt(tokenA[0]), Float.parseFloat(tokenA[1]) );
				}
				
				if(value.startsWith("B")){
					String[] tokenB = value.split(":")[1].split(",");
					mapB.put(Integer.parseInt(tokenB[0]), Float.parseFloat(tokenB[1]) );
				}
			}
			
			Iterator iterA = mapA.keySet().iterator();
			while(iterA.hasNext()){
				int idx = Integer.parseInt( iterA.next().toString() );
				float A = mapA.get(idx);
				float B = mapB.get(idx);
				pr += A * B;
//				System.out.println(idx + "         " + A + "        " + B);
			}
			context.write(key,new Text(prjob.scaleFloat(pr)));
		}
	}
	
	public static void main(Map<String, String> path) throws IOException, URISyntaxException, ClassNotFoundException, InterruptedException {
		
		String input = path.get("tmp1");
		String  output = path.get("tmp2");
		String pr = path.get("input_pr");
		
		hdfsGYT hdfs = new hdfsGYT();
		hdfs.rmr(output);
		
		Job job = new Job();
		job.setJarByClass(prJisuan.class);
		
		//set file input 
		FileInputFormat.setInputPaths(job, new Path(input), new Path(pr));
		job.setInputFormatClass(TextInputFormat.class);
		
		//set map
		job.setMapperClass(prJisuanMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);

		//set partition
		//set combine
		//set sort
		
		//set  reduce
		job.setReducerClass(prJisuanReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		//set outputpath
		FileOutputFormat.setOutputPath(job, new Path(output));
		job.setOutputFormatClass(TextOutputFormat.class);
		
		//upload job
        job.waitForCompletion(true);
        
        hdfs.rmr(pr);
        hdfs.rename(output, pr);
	}
}
