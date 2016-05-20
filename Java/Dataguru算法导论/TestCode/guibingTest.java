package TestCode;

import java.util.Arrays;
/*
 * 问题描述
 * 有N个整数，A[1]，A[2]，A[3]，....，A[N]。需要找打这样的（i，j）的数对的数量
 * 满足 1 <= i < j <=N, A[i] > A[j]。数据范围：1<= N <= 65537，0 <=A[i] <=10^9
 */
public class guibingTest {

	static int[] arr = {
		3,4,1,5,2,6              //示例数组
	};
	static int num = 0; //记录满足条件的对数
	
	public static void main(String[] args) {
		MergeSort(arr, 0, 5);
		System.out.println("满足条件的逆序数 共:   " + num + "对");

	}
	
	//归并排序寻找满足条件的对数
		private static void MergeSort(int[] arr, int low, int high) {
			// TODO Auto-generated method stub
			int mid = (low + high) /2;
			if(low<high){
				//拆分进行排序
				MergeSort(arr, low,mid);
				MergeSort(arr, mid+1, high);
				Merge(arr,low,mid+1,high);        //合并排序后的数据
			}
		}

		//合并函数
		private static void Merge(int[] arr, int low, int mid, int high) {
			// TODO Auto-generated method stub
			int len_L = mid-low; // 左数组的长度
			int len_R = high-mid+1; //右数组的长度
			int[] L = new int[len_L];     //定义左数组
			int[] R = new int[len_R ]; //定义右数组
			
			//给左数组赋值
			for(int i =0 ; i< len_L;i ++)
				L[i] = arr[low+i];
			
			//给右数组赋值
			for(int j =0 ;j< len_R; j++)
				R[j] = arr[mid + j];
			
			//比较L 和 R 数组,若满足条件 计数器+1
			for(int i =0 ; i < len_L; i ++)
				for(int j=0; j <len_R; j ++)
					if(L[i] <= R[j])
					{
						System.out.println(L[i] + "\t" + R[j]);
						num++;
					}
		}
		
}
