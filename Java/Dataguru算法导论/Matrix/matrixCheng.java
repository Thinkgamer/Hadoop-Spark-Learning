package Matrix;

/*
 * 方阵相乘
 * strassen，矩阵分块思想
 */
public class matrixCheng {
	//用于计算的两个数组
	static int[][] a = {
		{1,2,3},
        {2,3,4},
        {3,4,5}
		};
	static int[][] b = {
		{3,4,5},
		{4,5,6},
		{5,6,7}
		};
	static int[][] c = {		{0,0,0},		{0,0,0},  {0,0,0}		}; //用来存放a 与 b相乘的值

	public static void main(String[] args) {

		//正常计算规则计算
		normalCheng();
	}

	private static void normalCheng() {
//		 TODO Auto-generated method stub
		for(int line_a = 0 ; line_a< a.length; line_a ++ )               
		{
			for(int line_b =0 ; line_b< b.length; line_b++)
			{
				c[line_a][line_b] = 0;
				for (int k = 0 ;k< b[0].length; k ++)
				{
					c[line_a][line_b]= c[line_a][line_b] + a[line_a][k] * b[k][line_b];
				}
			}
		}
		printMatrix();
		
	}

	//打印出得到的乘积
	private static void printMatrix() {
		// TODO Auto-generated method stub
		//打印a
		System.out.println("a 矩阵：");
		for(int i =0; i< a.length; i++)
		{
			for(int j =0;j < a[0].length; j ++)
				System.out.print( a[i][j] + "\t");
			System.out.println();
		}
		//打印b
		System.out.println("b 矩阵：");
		for(int i =0; i< b.length; i++)
		{
			for(int j =0;j < b[0].length; j ++)
				System.out.print( b[i][j] + "\t");
			System.out.println();
		}
		//打印乘积矩阵			
		System.out.println("乘积矩阵：");
		for(int i =0 ; i< c.length; i ++)
		{
			for (int j =0;j<c[0].length; j ++)
				System.out.print( c[i][j] + "\t");
			System.out.println();
		}
	}
	
}
