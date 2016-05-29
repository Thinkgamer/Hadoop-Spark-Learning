package TestCode;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Hashtable;
import java.util.StringTokenizer;

/*
 * 题目描述：输入读应的几种字符串，其中一个是english，一个是外语开始是输入字典，后来是根据外语来查询字典，没有时输出"en"
 */

public class HashTableExample {
	public static void main(String[] args) throws IOException {
		BufferedReader stdin = new BufferedReader(new InputStreamReader(System.in));
		Hashtable <String, String> table = new Hashtable<String, String>();
		String s = "";
		String[] arr  = new String[2];
		while(true)
		{
			s = stdin.readLine();
			if(s.equals(""))
				break;
			arr=s.split(" ");
			table.put(arr[1],arr[0]);
		}
		while(true)
		{
			s = stdin.readLine();
			if(table.get(s) != null )
				System.out.println(table.get(s));
			else
				System.out.println("eh");
		}
	}
}
