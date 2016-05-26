package WebKPI;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.Map;

public class KPIJob {
    //定义全局变量 hdfs地址url	
	public static final String  HDFS = "hdfs://127.0.0.1:9000";
	
	public static void main(String[] args) throws ClassNotFoundException, IOException, InterruptedException, URISyntaxException {
		//定义一个map集合，存放程序中所需要的路径
		Map  <String, String> path= new HashMap<String, String>();
		
//		path.put("local_path", "webLogKPI/weblog/access.log");          //本地目录
		path.put("input_log", HDFS+"/mr/webLogKPI/log_files");  //hdfs上存放log的目录
		
		path.put("output_oneip", HDFS + "/mr/webLogKPI/KPI_OneIP_Sum");   //hdfs上KPI_OneIP_Sum对应的输出文件
		path.put("output_pv", HDFS + "/mr/webLogKPI/KPI_OnePV_Sum");   //hdfs上KPI_OnePV_Sum对应的输出文件
		path.put("output_request",HDFS+"/mr/webLogKPI/KPI_OneRequest_Sum");  //hdfs 上KPI_OneRequest_Sum对应的输出文件
		path.put("output_time", HDFS+"/mr/webLogKPI/KPI_OneTime_Sum");              //hdfs上KPI_OneTime_Sum对应的输出文件
		path.put("output_source", HDFS+"/mr/webLogKPI/KPI_OneResource_Sum");              //hdfs上KPI_OneResource_Sum对应的输出文件
		
		KPI_OneIP_Sum.main(path);    //计算独立IP访问量
		KPI_OnePV_Sum.main(path);    //计算PV访问量
		KPI_OneRequest_Sum.main(path);        //获得请求方式
		KPI_OneTime_Sum.main(path);          //每小时的PV
		KPI_OneSource_Sum.main(path);          //日访问设备统计
		
		System.exit(0);
	}
}
