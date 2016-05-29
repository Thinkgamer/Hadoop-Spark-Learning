package sort;

import java.util.Scanner;
/*
 * 插入排序
 * 时间复杂度： theta n^2
 */
public class insertSort {

	public static void main(String[] args) {
//		从键盘输入数组输入数组
//		Scanner scan = new Scanner(System.in);
//		int[] arr = new int[4];
//		for(int i =0;i<4;i++){
//			arr[i] = scan.nextInt();
//		}
		int[] arr ={3,1,5,2};
		//插入排序
		for(int i = 1; i<arr.length;i++){
			int key = arr[i];
			int j =i;
			while(j>0 && arr[j-1] > key){
				arr[j] = arr[j-1];
				j =  j-1;
			}
			arr[j] = key;
		}
		//输出数组
		System.out.println("排序后的数组为：");
		for(int i=0;i<arr.length;i++)
			System.out.print(arr[i] + "\t");
	}

}
