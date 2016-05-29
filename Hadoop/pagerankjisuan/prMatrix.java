package pagerankjisuan;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.Arrays;
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

public class prMatrix {
	
	private static int nums = 100; //页面数
	private static float d = 0.85f; //阻尼系数
	
	private static class MatrixMapper extends Mapper<LongWritable,Text,Text,Text>{
		private static final Text k = new Text();
		private static final Text v = new Text();
		public void map(LongWritable key,Text value, Context context) throws IOException, InterruptedException{
//			System.out.println(value.toString());
			String[] tokens = value.toString().split(",");
			k.set(tokens[0]);
			v.set(tokens[1]);
			context.write(k, v);
		}
	}
	
	
	public static class MatrixReducer extends Reducer<Text,Text,Text,Text>{
		
		public void reduce(Text key, Iterable<Text>values, Context context ) throws IOException, InterruptedException{
			float[] G = new float[nums];  //概率矩阵列
			Arrays.fill(G, (float)(1-d)   /	G.length );  //填充矩阵列
			
			float[] A = new float[nums] ; //近	邻矩阵列
			int  sum=0;      //链出数量
			for(Text text :values){
				int idx = Integer.parseInt(text.toString());
//				System.out.println(idx + "idx -=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=");
				A[idx-1 ] = 1;
				sum ++;
			}
			
			if(sum==0){ //分母不能为0
				sum=1;
			}
			
			StringBuilder sb = new StringBuilder();
			for(int i=0;i<A.length;i++){
				sb.append("," + (float) (G[i]  + d * A[i] / sum) );
			}
			
			Text v = new Text(sb.toString().substring(1));
//			System.out.println(key+ ":" + v.toString() );
			context.write(key, v);
		}
	}
	
	public static void main(Map<String, String> path) throws IOException, ClassNotFoundException, InterruptedException, URISyntaxException {
		
		String input = path.get("input");
        String input_pr = path.get("input_pr");
        String output = path.get("tmp1");
        
        String page = path.get("page");
        String pr = path.get("pr");

		hdfsGYT hdfs = new hdfsGYT();
		//创建需要的文件夹
		hdfs.rmr(input);
		hdfs.rmr(output);
		hdfs.mkdir(input);
		hdfs.mkdir(input_pr);
		//上传文件到指定的目录 内
		hdfs.put(page, input);
		hdfs.put(pr, input_pr);
		
		Job job = new Job();
		job.setJarByClass(prMatrix.class);
		
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		FileInputFormat.addInputPath(job, new Path(input));
		FileOutputFormat.setOutputPath(job, new Path(output));
		
		job.setMapperClass(MatrixMapper.class);
		job.setReducerClass(MatrixReducer.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

        job.waitForCompletion(true);
	}
}
