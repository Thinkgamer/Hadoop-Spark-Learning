package wordcount;

import java.io.IOException;
import java.io.StringReader;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.wltea.analyzer.core.IKSegmenter;
import org.wltea.analyzer.core.Lexeme;

public class wordcount {

	public static class Map extends Mapper<Object,Text,Text,IntWritable>{
		private static final Text word = new Text();
		public void map(Object key,Text value,Context context) throws IOException, InterruptedException{
			String line = value.toString();
			StringReader sr=new StringReader(line);  
	        IKSegmenter ik=new IKSegmenter(sr, true);  
	        Lexeme lex=null;  
	        while((lex=ik.next())!=null){  
				word.set(lex.getLexemeText());
				System.out.println(lex.getLexemeText() + "\tddddddddddddddddd\t" + "1");
				context.write(new Text(word),new IntWritable(1));
	        }  
		}
	}
	
	public static class Reduce extends Reducer<Text,IntWritable,Text,IntWritable>{
		private static final IntWritable result = new IntWritable();
		public void reduce(Text key,Iterable<IntWritable>values,Context context) throws IOException, InterruptedException{
			int num =0;
			for(IntWritable value:values){
				num += value.get();
			}
			result.set(num);
			System.out.println(key.toString() + "\t................." + num);
			context.write(key, result);
		}
	}
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		// TODO Auto-generated method stub
       Job job = new Job();
       job.setJarByClass(wordcount.class);
       
       job.setNumReduceTasks(1);   //设置reduce进程为1个，即output生成一个文件
       
       job.setMapperClass(Map.class);      
       job.setReducerClass(Reduce.class);
       
       job.setOutputKeyClass(Text.class);    //为job的输出数据设置key类
       job.setOutputValueClass(IntWritable.class);   //为job的输出设置value类
       
       FileInputFormat.addInputPath(job, new Path(args[0]));   //设置输入文件的目录
       FileOutputFormat.setOutputPath(job,new Path(args[1])); //设置输出文件的目录
       
       System.exit(job.waitForCompletion(true)?0:1);   //提交任务
	}
}