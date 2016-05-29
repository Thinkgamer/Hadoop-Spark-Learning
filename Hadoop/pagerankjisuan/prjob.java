package pagerankjisuan;

import java.text.DecimalFormat;
import java.util.HashMap;
import java.util.Map;

/*
 * 调度函数
 */
public class prjob {

	public static final String  HDFS = "hdfs://127.0.0.1:9000";
	
	public static void main(String[] args) {
		Map  <String, String> path= new HashMap<String, String>();	
		
		path.put("page" ,"/home/thinkgamer/MyCode/hadoop/MyItems/pagerankjisuan/people.csv");
		path.put("pr" ,"/home/thinkgamer/MyCode/hadoop/MyItems/pagerankjisuan/peoplerank.txt");
		
		path.put("input", HDFS + "/mr/blog_analysic_system/people");          // HDFS的目录
        path.put("input_pr", HDFS + "/mr/blog_analysic_system/pr");       // pr存储目录
        path.put("tmp1", HDFS + "/mr/blog_analysic_system/tmp1");             // 临时目录,存放邻接矩阵
        path.put("tmp2", HDFS + "/mr/blog_analysic_system/tmp2");           // 临时目录,计算到得PR,覆盖input_pr

        path.put("result", HDFS + "/mr/blog_analysic_system/result");                   // 计算结果的PR
        
        path.put("sort", HDFS +  "/mr/blog_analysic_system/sort");  //最终排序输出的结果
        
        try {
        	   dataEtl.main();
            prMatrix.main(path);
            int iter = 3;           // 迭代次数
            for (int i = 0; i < iter; i++) {
                prJisuan.main(path);
            }
           prNormal.main(path);
           prSort.main(path);

        	} catch (Exception e) {
        		e.printStackTrace();
        	}
        	System.exit(0);
    	}
	
	 	public static String scaleFloat(float f) {// 保留6位小数
	        DecimalFormat df = new DecimalFormat("##0.000000");
	        return df.format(f);
	    }
}