package WebKPI;

import java.io.IOException;
import java.net.URISyntaxException;
import java.text.ParseException;
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
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class KPI_OneRequest_Sum {

	private static KPIfilter kpi;   //声明一个kpi对象
	//Mapper类
	public static class ReMap extends Mapper<LongWritable , Text, Text , LongWritable>
	{
		String filename;       //读取的文件名
		static Text reK1 = new Text();
		static LongWritable reV1 = new LongWritable(1);
		
		//setup函数，没个Map执行一次
		@Override
		protected void setup(Context context)throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			InputSplit split = context.getInputSplit();
			filename =  ((FileSplit) split).getPath().getName();
			filename = filename.substring(0, 11).replace("-", "");     //得到合法的文件名
			System.out.println("filename： " + filename);
		}
		//map函数
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
		{
			try {
				kpi = KPIfilter.parser(value.toString());
				if(kpi.isValid())
				{
					reK1.set(kpi.getRequest()+"\t"+filename);
					context.write(reK1, reV1);
				}
			} catch (ParseException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}
	//Reducer类
	public static class ReReduce extends Reducer<Text, LongWritable, Text, LongWritable>
	{
		private static Text reK2 = new Text();                                               //key
		private static LongWritable reV2 = new LongWritable();          //value
		
		private MultipleOutputs<Text, LongWritable> mos;                         //声明多路输出
		//setup函数
		@Override
		protected void setup(Context context) throws IOException, InterruptedException {
			mos = new MultipleOutputs<Text, LongWritable >(context);
		}
		//reduce函数
		public void reduce(Text key, Iterable<LongWritable> values, Context contexts) throws IOException, InterruptedException
		{
			int sum=0;
			String[] arr = key.toString().split("\t");
			for (LongWritable w : values) {
				sum += w.get();
			}
			reK2.set(arr[0]);
			reV2.set(sum);
			mos.write(arr[1], reK2, reV2);
		}
		//cleanup函数
		@Override
		protected void cleanup(Context context) throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			mos.close();
		}
	}

	public static void main(Map<String, String> path) throws IOException, URISyntaxException, ClassNotFoundException, InterruptedException {
		// TODO Auto-generated method stub
	
		String hdfs_input = path.get("input_log");                     //指定输入输出文件夹
		String hdfs_output = path.get("output_request");
		
		hdfsGYT hdfs = new hdfsGYT();
		hdfs.rmr(hdfs_output);               //首先删除对应的hdfs上的文件输出目录
		
		Job job = new Job(new Configuration(), "RequestSum");
		job.setJarByClass(KPI_OneRequest_Sum.class);
		
		job.setMapperClass(ReMap.class);
		job.setReducerClass(ReReduce.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(LongWritable.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(LongWritable.class);
		
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		MultipleOutputs.addNamedOutput(job, "17Apr2016", TextOutputFormat.class, Text.class, LongWritable.class);
		MultipleOutputs.addNamedOutput(job, "18Apr2016", TextOutputFormat.class, Text.class, LongWritable.class);
		MultipleOutputs.addNamedOutput(job, "19Apr2016", TextOutputFormat.class, Text.class, LongWritable.class);
		MultipleOutputs.addNamedOutput(job, "20Apr2016", TextOutputFormat.class, Text.class, LongWritable.class);
		MultipleOutputs.addNamedOutput(job, "21Apr2016", TextOutputFormat.class, Text.class, LongWritable.class);
		MultipleOutputs.addNamedOutput(job, "22Apr2016", TextOutputFormat.class, Text.class, LongWritable.class);
		MultipleOutputs.addNamedOutput(job, "23Apr2016", TextOutputFormat.class, Text.class, LongWritable.class);
		MultipleOutputs.addNamedOutput(job, "24Apr2016", TextOutputFormat.class, Text.class, LongWritable.class);
		MultipleOutputs.addNamedOutput(job, "25Apr2016", TextOutputFormat.class, Text.class, LongWritable.class);
		MultipleOutputs.addNamedOutput(job, "26Apr2016", TextOutputFormat.class, Text.class, LongWritable.class);
		MultipleOutputs.addNamedOutput(job, "27Apr2016", TextOutputFormat.class, Text.class, LongWritable.class);
		MultipleOutputs.addNamedOutput(job, "28Apr2016", TextOutputFormat.class, Text.class, LongWritable.class);
		MultipleOutputs.addNamedOutput(job, "29Apr2016", TextOutputFormat.class, Text.class, LongWritable.class);
		MultipleOutputs.addNamedOutput(job, "30Apr2016", TextOutputFormat.class, Text.class, LongWritable.class);
		
		FileInputFormat.addInputPath(job, new Path(hdfs_input));
		FileOutputFormat.setOutputPath(job, new Path(hdfs_output) );
		
		//提交作业
		job.waitForCompletion(true);

		//
		System.out.println("User_agent Error:" + kpi.getNumUser_agent());
		System.out.println("Status Error:" + kpi.getStatus());
	}
	
}
