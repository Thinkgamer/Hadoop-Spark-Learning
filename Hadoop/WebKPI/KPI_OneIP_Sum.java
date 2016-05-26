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
/*
 * 2016.04.17——2016.04.30期间的数据
 * 分别统计每天的ip独立访问数目
*/

public class KPI_OneIP_Sum {
	private static KPIfilter kpi = new KPIfilter();
	
	private static class IPMapper extends Mapper<LongWritable,Text,Text,LongWritable>{
		private Text ipK = new Text();
		private LongWritable ipV = new LongWritable(1);
		String filename;
		
		public void setup(Context context) throws IOException,InterruptedException {
			// TODO Auto-generated method stub
			//获取文件名
			InputSplit inputSplit = context.getInputSplit();
			filename = ((FileSplit) inputSplit).getPath().getName();
			filename = (String) filename.subSequence(0,11);
			System.out.println(filename);
		}

		public void map(LongWritable key,Text value, Context context) throws IOException, InterruptedException{
//			System.out.println(value.toString());
			
			try {
				kpi = KPIfilter.parser(value.toString());
				//在进行判断时
				if(kpi.isValid()){
					ipK.set(kpi.getRemote_ip() + "\t" + filename);
//					System.out.println(ipK + "===" + ipV);
					context.write(ipK, ipV);
				}
			} catch (ParseException e) {
				// TODO Auto-generated catch block
//				e.printStackTrace();
				System.out.println("this some error");
			}
			
		}
	}
	
	public static class IPReducer extends Reducer<Text,LongWritable,Text,LongWritable>{
		private Text ipK2 = new Text();
		private LongWritable ipV2 = new LongWritable(); 
		
		//声明mos变量，用来将来自不同文件的ip统计写入到不同的文件中
		private MultipleOutputs<Text, LongWritable> mos;
		//setup函数
		protected void setup(Context context) throws IOException,InterruptedException {
			// TODO Auto-generated method stub
			mos = new MultipleOutputs<Text, LongWritable>(context);
		}
		
		public void reduce(Text key, Iterable<LongWritable>values, Context context ) throws IOException, InterruptedException{
				String[] arr = key.toString().split("\t");
				String filename = arr[1].replace("-","");          //该数据来自哪个文件, 因为文件命名时不能出现-，所有都去掉
//				System.out.println(filename);
				
				//统计每个文件下每个IP出现的次数
				int num=0;
				for (LongWritable longWritable : values) {
					num += longWritable.get();
				}

				ipK2.set(arr[0]);
				ipV2.set(num);
//				System.out.println(filename + "______________"  + ipK2 + "===========" + ipV2);
				mos.write(filename, ipK2, ipV2);
		}
		
		//cleanup函数  关闭mos
		public void cleanup(Context context) throws IOException,InterruptedException {
				mos.close();
			}
	}
	
	public static void main(Map<String, String> path) throws IOException, ClassNotFoundException, InterruptedException, URISyntaxException {
		
//		String local_input = path.get("local_path");    //存放log的本地目录
		String hdfs_input = path.get("input_log");   //上传weblog到hdfs的目录
		String hdfs_output = path.get("output_oneip");  //运行该job的输出目录
		
		hdfsGYT hdfs = new hdfsGYT();
//		hdfs.rmr(hdfs_input);     //删除hdfs上weblog的存放目录
		hdfs.rmr(hdfs_output);  //删除hdfs上任务的输出目录
//		hdfs.put(local_input, hdfs_input);   //将weblog从本地上传至hdfs	  
		
		Job job = new Job(new Configuration(),"OneIP_Sum");
		job.setJarByClass(KPI_OneIP_Sum.class);
		
		job.setMapperClass(IPMapper.class);      
	    job.setReducerClass(IPReducer.class);
		
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

