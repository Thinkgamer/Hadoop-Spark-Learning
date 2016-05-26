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


public class KPI_OneSource_Sum {

	private static KPIfilter kpi;   //声明一个kpi对象
	
	public static class SourceMap extends Mapper<LongWritable, Text, Text, LongWritable>{

		static String filename;       //存储文件名
		static Text  sK1 = new Text(); //key
		static  LongWritable sV1 = new LongWritable(1);     //value
		
		@Override
		protected void setup(Context context) throws IOException,InterruptedException {
			// TODO Auto-generated method stub
			InputSplit split = context.getInputSplit();
			filename =  ((FileSplit) split).getPath().getName();
			filename = filename.substring(0, 11).replace("-", "");     //得到合法的文件名
			System.out.println("filename： " + filename);
		}

		//map函数
		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			try {
				kpi = KPIfilter.parser(value.toString());
				if(kpi.isValid())
				{
					sK1.set(kpi.getUser_agent()+"\t"+filename);
					context.write(sK1, sV1);
				}
			} catch (ParseException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		
	}
	
	public static class SourceReduce extends Reducer< Text, LongWritable, Text , LongWritable>{

		static Text sK2 = new Text();                     //key
		static LongWritable sV2 = new LongWritable();        //value

		private MultipleOutputs<Text, LongWritable> mos;                         //声明多路输出
		
		@Override
		protected void setup(Context context)throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			mos =new MultipleOutputs<Text, LongWritable> (context); 
		}

		@Override
		protected void reduce(Text key, Iterable<LongWritable> values,Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			int sum=0;
			String[] arr = key.toString().split("\t");
			for (LongWritable w : values) {
				sum += w.get();
			}
			sK2.set(arr[0]);
			sV2.set(sum);
			System.out.println(arr[1]);
			mos.write(arr[1], sK2, sV2);
		}
		@Override
		public void cleanup(Context context)throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			mos.close();
		}
		
	}
	
	public static void main(Map<String, String> path) throws IOException, URISyntaxException, ClassNotFoundException, InterruptedException {
		// TODO Auto-generated method stub
		String hdfs_input = path.get("input_log");                     //指定输入输出文件夹
		String hdfs_output = path.get("output_source");
		
		hdfsGYT hdfs = new hdfsGYT();
		hdfs.rmr(hdfs_output);               //首先删除对应的hdfs上的文件输出目录
		
		Job job = new Job(new Configuration(), "Resource");
		job.setJarByClass(KPI_OneSource_Sum.class);
		
		job.setMapperClass(SourceMap.class);
		job.setReducerClass(SourceReduce.class);
		
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
