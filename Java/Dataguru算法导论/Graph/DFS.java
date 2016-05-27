package Graph;

import java.util.ArrayList;
import java.util.List;

/*
 * 深度优先遍历算法
 * DFS
 */
public class DFS {

	private static Object[] vet; //定义vet数组用来存放顶点信息
	private static int[][] array;  //定义邻接矩阵用来存放图的顶点信息
	private static int vexnum;      //存放边的条数
	private static boolean[] ifvisited; //存放节点是否被访问过
	private static List<Object> list = new ArrayList<Object>();  //定义一个临时的队列用来存放已经被访问过的节点

	public static void main(String[] args) {
		DFS map = new DFS(5); //初始化队列
		Character[] vet = {'A','B','C','D','E'};
		map.addVet(vet);   //添加顶点
		map.addEage(0,1);
		map.addEage(0,4);
		map.addEage(1,3);
		map.addEage(2,3);
		map.addEage(2,4);
		
		System.out.println("深度优先遍历开始...");
		visited(0);
		ifvisited[0]=true;
		map.dfs(0);
	}
	
	//深度优先遍历
	private void dfs(int k) {
		// TODO Auto-generated method stub
		for(int i=0; i< vexnum; i++)
			if(array[k][i] == 1 && !ifvisited[i])//判断是否被访问过，且其值是否为1
			{
				ifvisited[i] = true;
				visited(i);   //添加到被访问过的节点队列
				for(int j=0; j<vexnum; j++)
				{
					if(!ifvisited[j] && array[i][j] ==1)
					{
						ifvisited[j] = true;
						visited(j);
						dfs(j);  //下次循环从vet[j]开始循环
					}
				}
			}
	}

	//往临时队列里添加已经访问过的结点，并输出
	private static void visited(int k) {
		// TODO Auto-generated method stub
		list.add(vet[k]);
		System.out.println("   -> " + vet[k]);
	}

	//构建邻接矩阵，保存边的信息
	private void addEage(int m, int n) {
		// TODO Auto-generated method stub
		if(m!=n){
			array[m][n] =1;
			array[n][m] =1;
		}
		else
			return;
	}
	
	//初始化图的顶点
	private void addVet(Character[] vet2) {
		// TODO Auto-generated method stub
		this.vet = vet2;
	}

	//图的初始化
	public DFS(int num) {
		// TODO Auto-generated constructor stub
		vexnum = num;   //顶点
		vet = new Object[num]; //顶点的信息
		array = new int[num][num];  //边的信息
		ifvisited = new boolean[num]; //是否被访问过
		for(int i =0 ;i< num; i++)    //初始化边
		{
			ifvisited[i] = false;
			for(int j =0;j<num;j++)
				array[i][j]=0;
		}
	}


}
