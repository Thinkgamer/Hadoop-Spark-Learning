package Matrix;

/*
 * 查找数组中的最大字串
 */

public class maxArr {

	static int[] arr = {
			1, -2, 3, 10, -4, 7, 2, -5				//定义数组，含有正数和负数	        
	};
	static int maxIndex = arr.length-1;   //数组的最大下标
	
	public static void main(String[] args) {
		//查找最大子串的两种算法
		findMaxArr2();
		System.out.println("\n=====================");
		findMaxArr3();
}

	//算法复杂度 n(n-1)
	private static void findMaxArr2() 
	{
		// TODO Auto-generated method stub
		
		int max = arr[0];
		int sum = 0;
		int startIndex = 0;       //记录最大字串的起始位置
		int endIndex = 0;         //记录最大字串的结束位置
		for(int i =0 ;i<maxIndex;i++)
		{
			sum = 0;
			for(int j = i ; j < maxIndex; j ++)
			{
				sum += arr[j];
				if(sum > max)
				{
					max = sum;
				    startIndex = i;
					endIndex = j;
				}
			}
		}
		System.out.println("Max sum is :" + max);      //输出最大子数组和
		printMaxArr(startIndex, endIndex);                 //输出最大子数组
	}
	
	//算法复杂度 n
	private static void findMaxArr3()
	{
		// TODO Auto-generated method stub
		int max = arr[0];
		int sum = 0;
		int startIndex = 0;       //记录最大子串的起始位置
		int endIndex = 0 ;   // 记录最大子串的结束位置
		for ( int i =0 ; i< maxIndex; i ++)
		{
			if ( sum >= 0)
			{
				sum += arr[i];
			}
			else
			{
				sum = arr[i];
				startIndex = i;
			}
			if(sum > max)
			{
				max = sum;
				endIndex = i;
			}
		}
		System.out.println("Max sum is :" + max);
		printMaxArr(startIndex, endIndex);
		
	}

	//输出最大子数组
	private static void printMaxArr(int startIndex, int endIndex) {
		// TODO Auto-generated method stub
		for(int i =startIndex ; i<= endIndex; i ++)
			System.out.print( arr[i] + "\t");
	}
	
	
}
