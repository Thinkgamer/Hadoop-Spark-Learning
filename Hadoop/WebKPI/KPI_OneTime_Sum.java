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

public class KPI_OneTime_Sum {
	
	private static KPIfilter kpi;   //声明一个kpi对象
	
	public static class OneTimeMap extends Mapper<LongWritable, Text, Text, LongWritable>{
		String filename;       //读取的文件名
		static Text timeK1 = new Text();
		static LongWritable timeV1 = new LongWritable(1);
		
		//setup函数，没个Map执行一次
		@Override
		protected void setup(Context context) throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			InputSplit split = context.getInputSplit();
			filename =  ((FileSplit) split).getPath().getName();
			filename = filename.substring(0, 11).replace("-", "");     //得到合法的文件名
			System.out.println("filename： " + filename);
		}
		
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			try {
				kpi = KPIfilter.filterPVs(value.toString());
				if(kpi.isValid()){
					timeK1.set(kpi.getTime_local_Date_hour()+"\t" + filename);
					context.write(timeK1,timeV1);
				}
			} catch (ParseException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}
	
	public static class OneTimeReduce extends Reducer<Text , LongWritable, Text, LongWritable>{
		
		private static Text timeK2 = new Text();
		private static LongWritable timeV2 = new LongWritable();
		
		private MultipleOutputs<Text, LongWritable> mos;                         //声明多路输出
		//setup函数
		@Override
		protected void setup(Context context) throws IOException, InterruptedException {
			mos = new MultipleOutputs<Text, LongWritable >(context);
		}
		
		public void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException{
			int sum = 0;
			String[] arr = key.toString().split("\t");
			String filename =arr[1];
			for (LongWritable longWritable : values) {
				sum += longWritable.get();
			}
			timeK2.set(arr[0]);
			timeV2.set(sum);
			mos.write(filename, timeK2, timeV2);
		}
		
		public void cleanup(Context context) throws IOException, InterruptedException{
			mos.close();
		}
	}

	public static void main(Map<String, String> path) throws IOException, URISyntaxException, ClassNotFoundException, InterruptedException {
		// TODO Auto-generated method stub
		String hdfs_input = path.get("input_log");          //loghdfs存放
		String hdfs_output = path.get("output_time");   //pv输出的目录
		
		hdfsGYT hdfs = new hdfsGYT();
		hdfs.rmr(hdfs_output);    //如果存在输出的目录的首先删除，否则会报错
		
		Job job = new Job(new Configuration(), "OneTime");
		job.setJarByClass(KPI_OnePV_Sum.class);
		
		job.setMapperClass(OneTimeMap.class);
		job.setReducerClass(OneTimeReduce.class);
		
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
		FileOutputFormat.setOutputPath(job, new Path(hdfs_output));
		
		//提交作业
		job.waitForCompletion(true);
		
		//
		System.out.println("User_agent Error:" + kpi.getNumUser_agent());
		System.out.println("Status Error:" + kpi.getStatus());
	}

}
