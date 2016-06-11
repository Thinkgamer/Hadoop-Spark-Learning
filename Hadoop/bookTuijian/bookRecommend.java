package bookTuijian;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Pattern;


public class bookRecommend {

	/**
	 * @param args
	 * 驱动程序，控制所有的计算结果
	 */
	public static final String  HDFS = "hdfs://127.0.0.1:9000";
	public static final Pattern DELIMITER = Pattern.compile("[\t,]");
	
	public static void main(String[] args) throws IOException, URISyntaxException, ClassNotFoundException, InterruptedException {
		// TODO Auto-generated method stub
		Map<String,String>  path = new HashMap<String,String>();
		path.put("local_file", "MyItems/bookTuijian/score.txt");          //本地文件所在的目录
		path.put("hdfs_root_file", HDFS+"/mr/bookRecommend/score");       //上传本地文件到HDFS上的存放路径
		
		path.put("hdfs_step1_input", path.get("hdfs_root_file"));       //step1的输入文件存放目录
		path.put("hdfs_step1_output", HDFS+"/mr/bookRecommend/step1"); //hdfs上第一步运行的结果存放文件目录
		
		path.put("hdfs_step2_input", path.get("hdfs_step1_output"));           //step2的输入文件目录
		path.put("hdfs_step2_output", HDFS+"/mr/bookRecommend/step2");      //step2的输出文件目录
		
		path.put("hdfs_step3_1_input", path.get("hdfs_step1_output"));        //构建评分矩阵
		path.put("hdfs_step3_1_output", HDFS+"/mr/bookRecommend/Step3_1");
		
		path.put("hdfs_step3_2_input", path.get("hdfs_step2_output"));        //构建同现矩阵
		path.put("hdfs_step3_2_output", HDFS+"/mr/bookRecommend/Step3_2");
		
		path.put("hdfs_step4_input_1", path.get("hdfs_step3_1_output"));    //计算乘积
		path.put("hdfs_step4_input_2", path.get("hdfs_step3_2_output"));
		path.put("hdfs_step4_output", HDFS+"/mr/bookRecommend/result");
		
		path.put("hdfs_step4_updata_input",path.get("hdfs_step3_1_output")); //Step4进行优化
		path.put("hdfs_step4_updata2_input",path.get("hdfs_step3_2_output"));
		path.put("hdfs_step4_updata_output", HDFS+"/mr/bookRecommend/Step4_Updata");
		
		path.put("hdfs_step4_updata2_input",path.get("hdfs_step4_updata_output")); //Step4进行优化
		path.put("hdfs_step4_updata2_output", HDFS+"/mr/bookRecommend/Step4_Updata2");
		
		
//	    Step1.run(path); 
//	    Step2.run(path); 
//		Step3_1.run(path);   //构造评分矩阵
//		Step3_2.run(path);   //构造同现矩阵
//		Step4.run(path);       //计算乘积
//		Step4_Updata.run(path);
	    Step4_Updata2.run(path);	
	    System.exit(0);
	}

}
