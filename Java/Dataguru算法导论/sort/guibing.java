package sort;

import java.util.Arrays;

/*
 *归并排序
 *时间复杂度 n*lg n 
 */

public class guibing {

	public static void main(String[] args) {
		//定义数组
		int[] arr = {6,2,4,1,9,65,23,12};
		
		//调用归并排序算法
		MergeSort(arr,0,7);
		
		System.out.println(Arrays.toString(arr));
	}

	//归并排序算法
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
		int[] L = new int[len_L+1];     //定义左数组
		int[] R = new int[len_R +1]; //定义右数组
		
		//给左数组赋值，最后一个元素赋予最大值，避免所有元素比较后，另一个数组的所有元素能存入新的数组
		for(int i =0 ; i< len_L;i ++)
			L[i] = arr[low+i];
		L[len_L] = Integer.MAX_VALUE;
		
		//给右数组赋值，最后一个元素赋予最大值
		for(int j =0 ;j< len_R; j++)
			R[j] = arr[mid + j];
		R[len_R] = Integer.MAX_VALUE;
		
		//比较L 和 R 数组，将较小的元素赋值给新数组
		int i = 0;
		int j = 0;
		for ( int k = low;k < high+1 ; k++){
			if ( L[i] < R [j]){
				arr[k] = L[i];
				i++;
			}
			else{
				arr[k] = R [j];
			    j++;
			}
		}
//		System.out.println(Arrays.toString(arr));
	}
	
}
