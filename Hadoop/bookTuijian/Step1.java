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

/*
 * Step1：得到评分矩阵
 */
public class Step1 {

	//Map类
	public static class Step1_Map extends Mapper<LongWritable, Text, Text, Text>{

		String filename; //存放文件名字
		static Text k1 = new Text(); //key
		static Text v1 = new Text();//value
		
		//setup函数，每次运行Map类只执行一次,获取并打印文件名
		@Override
		protected void setup(Context context) throws IOException,InterruptedException {
			// TODO Auto-generated method stub
			InputSplit inputsplit = context.getInputSplit();
			filename = ((FileSplit) inputsplit).getPath().getName();
			System.out.println("Filename：" + filename);
		}

		@Override
		protected void map(LongWritable key, Text value, Context context)throws IOException, InterruptedException {
			// TODO Auto-generated method stub
//			String[] arr = value.toString().split(",");
			String[] arr = value.toString().split("\t");
			k1.set(arr[0]);
			v1.set(arr[2]+":"+arr[1]);
//			v1.set(arr[1]+":"+arr[2]);
			context.write(k1, v1);
		}
		
	}
	
	//Reduce类
	public static class Step1_Reduce extends Reducer<Text,Text, Text, Text>{
		static Text k2 = new Text();
		static Text v2 = new Text();
		@Override
		protected void reduce(Text key, Iterable<Text> values,Context context)throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			String id_score="";
			for (Text text : values) {
				id_score += "," + text.toString();
			}
			id_score = id_score.substring(1);
			k2.set(key);
			v2.set(id_score);
			context.write(k2, v2);
		}
		
	}
	
	public static void run(Map<String, String> path) throws IOException, URISyntaxException, ClassNotFoundException, InterruptedException {
		// TODO Auto-generated method stub
		String local_path = path.get("local_file");       //存放文件的本地目录
		String hdfs_file_path = path.get("hdfs_root_file");   //hdfs上存放文件的目录
		String input_path = path.get("hdfs_step1_input");  //step1的输入文件目录
		String output_path = path.get( "hdfs_step1_output"); //step2的输出文件目录
		System.out.println(local_path);
		System.out.println(hdfs_file_path);
		System.out.println(input_path);
		System.out.println(output_path);
		
		hdfsGYT hdfs = new hdfsGYT();           //声明一个hdfs的操作对象
		hdfs.rmr(input_path);           //若输入的文件目录存在则删除
		hdfs.rmr(output_path);     //若输出的文件目录存放则删除
		hdfs.put(local_path, input_path);   //将本地文件上传至hdfs
		
		Job job = new Job(new Configuration(), "BookRecommend");
		job.setJarByClass(Step1.class);
		
		//设置文件路径
		FileInputFormat.setInputPaths(job, new Path(input_path));
		FileOutputFormat.setOutputPath(job, new Path(output_path));
	
		//设置Map和Reduce类
		job.setMapperClass(Step1_Map.class);
		job.setReducerClass(Step1_Reduce.class);
	
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
