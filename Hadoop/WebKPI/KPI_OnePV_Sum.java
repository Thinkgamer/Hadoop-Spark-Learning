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

public class KPI_OnePV_Sum {
	
	private static KPIfilter kpi; //声明一个KPIfilter对象
	
	//Mapper类
	public static class PVMap extends Mapper<LongWritable, Text, Text, LongWritable>{

		private static String filename ;//整个Map函数使用这个变量，意思为 获取当前文件的名称
		private static Text pvK1 = new Text();
		private static LongWritable pvV1 = new LongWritable(1);
		
		//获取文件名，setup函数,每次执行一个Map类时只调用一次
		@Override
		protected void setup(Context context) throws IOException,InterruptedException {
			// TODO Auto-generated method stub
			InputSplit input = context.getInputSplit();
			filename = ((FileSplit) input).getPath().getName();             //获得的是形如 26-Apr-2016.txt
			filename = filename.substring(0, 11).replace("-","");           //转换为： 26Apr2016
			System.out.println("filename：" + filename);
		}
		
		public void map(LongWritable key, Text value ,Context context) throws IOException, InterruptedException{
			try {
				kpi = KPIfilter.filterPVs(value.toString());
				if(kpi.isValid())
				{
					pvK1.set(kpi.getSee_url() + "\t" + filename);           //key设置为从log中解析出的访问入口
					context.write(pvK1, pvV1);
				}
			} catch (ParseException e) {
				// TODO Auto-generated catch block
//				e.printStackTrace();
//				System.out.println("This is some error");
			}
		}
		
	}
	//Reducer类
	public static class PVReduce extends Reducer<Text,LongWritable,Text,LongWritable>{
		
		private static Text pvk2 = new Text();          //key
		private static LongWritable pvV2 = new LongWritable(); //value
		
		//声明mos变量，将不同日期的处理结果写进不同的文件
		private MultipleOutputs<Text, LongWritable> mos;
		
		//reduce类中的setup函数
		@Override
		protected void setup(org.apache.hadoop.mapreduce.Reducer.Context context)	throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			mos = new MultipleOutputs<Text, LongWritable>(context);
		}
		
		public void reduce(Text key, Iterable<LongWritable> values, Context contexts) throws IOException, InterruptedException{
			String[] arr = key.toString().split("\t");
			String filename = arr[1];
			
			//统计每个指定页面的日访问量
			int seeNum = 0;
			for(LongWritable w : values)
			{
				seeNum += w.get();
			}
			
			pvk2.set(arr[0]);
			pvV2.set(seeNum);
//			System.out.println(filename + "______________"  + pvk2 + "===========" + pvV2);
			mos.write(filename, pvk2, pvV2);
		}
		
		//cleanup函数  关闭mos
				public void cleanup(Context context) throws IOException,InterruptedException {
						mos.close();
					}
	}
	
	//main函数
	public static void main(Map<String, String> path) throws IOException, ClassNotFoundException, InterruptedException, URISyntaxException {
		String hdfs_input = path.get("input_log");          //loghdfs存放
		String hdfs_output = path.get("output_pv");   //pv输出的目录
		
		hdfsGYT hdfs = new hdfsGYT();
		hdfs.rmr(hdfs_output);    //如果存在输出的目录的首先删除，否则会报错
		
		Job job = new Job(new Configuration(), "PV");
		job.setJarByClass(KPI_OnePV_Sum.class);
		
		job.setMapperClass(PVMap.class);
		job.setReducerClass(PVReduce.class);
		
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
