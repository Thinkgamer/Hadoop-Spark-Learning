package pagerankjisuan;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;

public class dataEtl {

	public static void main() throws IOException {

		File f1 = new File("MyItems/pagerankjisuan/people.csv");
		if(f1.isFile()){
			f1.delete();
		}
		File f = new File("MyItems/pagerankjisuan/peoplerank.txt");
		if(f.isFile()){
			f.delete();
		}
		//打开文件
		File file = new File("MyItems/pagerankjisuan/day7_author100_mess.csv");
		//定义一个文件指针
		BufferedReader reader = new BufferedReader(new FileReader(file));
		try {
			String line=null;
			//判断读取的一行是否为空
			while( (line=reader.readLine()) != null)
			{
					String[] userMess = line.split( "," );
					//第一字段为id，第是个字段为粉丝列表
					String userid = userMess[0];
					if(userMess.length!=0){
							if(userMess.length==11)
							{
									int i=0;
									String[] focusName = userMess[10].split("\\|"); //  | 为转义符
									for (i=1;i < focusName.length; i++) 
										{
											write(userid,focusName[i]);
//											System.out.println(userid+ "           " + focusName[i]);
										}
							}
							else
							{
									int j =0;
									String[] focusName = userMess[9].split("\\|"); //  | 为转义符
									for (j=1;j < focusName.length; j++) 
									{
										write(userid,focusName[j]);
//										System.out.println(userid+ "           " + focusName[j]);
									}
							}		
					}
				}
			} 
			catch (FileNotFoundException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			finally
			{
					reader.close();
				
					//etl peoplerank.txt
					for(int i=1;i<=100;i++){
						FileWriter writer = new FileWriter("MyItems/pagerankjisuan/peoplerank.txt",true);
						writer.write(i + "\t" + 1 + "\n");
						writer.close();
					}
			}
			System.out.println("OK..................");
	}

	private static void write(String userid, String nameid) {
		// TODO Auto-generated method stub
		//定义写文件，按行写入
		try {
			if(!nameid.contains("\n")){
				FileWriter writer = new FileWriter("MyItems/pagerankjisuan/people.csv",true);
				writer.write(userid + "," + nameid + "\n");
				writer.close();
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

}
