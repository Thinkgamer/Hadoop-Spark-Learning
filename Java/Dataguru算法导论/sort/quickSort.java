package sort;

import java.util.Arrays;

public class quickSort {
	public static int a[] = {6,1,2,7,9,3,4,5,10,8};   //定义全局数组a
	
	public static void Sort(int low,int high){
		if(low>high)
			return;
		int temp; //保存基准值
		int left=0,right=0,empty=0;
		temp=a[low];                   //将每次进来的最左边的值作为基准值
		left = low;                     //每次移动的指针初始值最左边的位置复制给left
		right = high;					//每次移动的指针初始值最右边的位置复制给right
		while(left!=right){             //判断循环结束的条件
			while(a[right]>=temp && left<right)           //找到比基准数小的数,先从右边开始寻找
				right--; 
			while(a[left]<=temp && left<right)			 //找到比基准数大的数
				left++;
			if(left<right) {// 交换数，保证基准数左边的数比基准数小，右边的数比基准数大
			empty = a[left];
			a[left]=a[right];
			a[right]=empty;
			}
		}
		    //交换基准数
		a[low]=a[left];
		a[left]=temp;
		Sort(low,left-1);    //递归处理基准数左边
		Sort(left+1,high);   //递归处理基准数右边
	}
	
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		Sort(0,9);  //进入快速排序
		System.out.println(Arrays.toString(a));
	}

}
