package BitTree;

import java.util.Scanner;

public class tree {
	
	Scanner input = new Scanner(System.in); //声明一个从键盘输入的对象

	static final int MAXLen = 20 ;          //定义最大长度
	class CBType{
		String data;                 //节点值
		CBType left;           //左孩子引用
		CBType right;      //右孩子引用
	}
	
	/*
	 * 初始化二叉树
	 */
	CBType initTree()
	{
		CBType node;
		if(    (node = new CBType())!=null     )               //如果内存分配成功
		{
			System.out.println("请先输入一个根节点数据：");
			node.data = input.next();     //初始化节点的各部分值
			node.left = null;
			node.right = null;
			if(node!=null)
				return node;
			else
				return null;
		}
		return null;
	}
	
	/*
	 * 添加节点
	 */
	void addTreeNode(CBType treeNode)
	{
		CBType pnode,parent;
		String data;
		int menusel;
		
		if( ( pnode = new CBType())!=null )
		{
			System.out.println("请输入二叉树的节点数据：");
			pnode.data = input.next();
			pnode.left = null;
			pnode.right = null;
			
			System.out.println("输入该节点的父结点的数据：");
			data = input.next();
			parent = TreeFindNode(treeNode,data);                //查找指定数据的节点
			if(parent == null)
			{
				System.out.println("未找到该父结点！");
				pnode = null;  //如果未找到，释放该节点
				return;
			}
			//如果找到的话进行以下操作
			System.out.println("1:添加该节点到左子树\n2:添加该节点到右子树");
			do{
				menusel = input.nextInt();                 //输入1或者2
				
				if(menusel==1 || menusel==2)
				{
					if(parent==null)
					{
						System.out.println("不存在父结点，请先设置父结点");
					}
					else
					{
						switch(menusel)
						{
						
						case 1:           //添加到左子树		
							if(parent.left != null)
							{
								System.out.println("左子树节点不能为空");
							}
							else
							{
								parent.left=pnode;
							}
							break;
							
						case 2:
							if(parent.right != null )
							{
								System.out.println("右子树节点不能为空");
							}
							else
							{
								parent.right=pnode;
							}
							break;
							default:
								System.out.println("无效的参数！");
						}
					}
				}
			}while(menusel != 1 && menusel != 2);
		}
	}
	/*
	 * 查找节点，分别递归查找左子树和右子树。逐个进行比较，若找到目标数据，则返回该数据所在的节点的引用
	 */
	CBType TreeFindNode(CBType treeNode, String data) {
		CBType ptr;
		
		if(treeNode==null){
			return null;
		}
		else
		{
			if(treeNode.data.equals(data))
			{
				return treeNode;
			}
			else
			{
				if( (ptr=TreeFindNode(treeNode.left, data)) != null  )   //递归查找左子树
				{
					return ptr;
				}
				else if( (ptr = TreeFindNode(treeNode.right, data))!= null )  //递归查找右子树
				{
					return ptr;
				}
				else   //如果在左右子树中均没有找到，则返回null
				{
					return null;
				}
			}
		}
	}
	
	/*
	 * 获取左子树
	 */
	CBType TreeLeftNode(CBType treeNode)
	{
		if(treeNode!=null)
			return treeNode.left;
		else
			return null;		
	}
	/*
	 * 获取右子树
	 */
	CBType TreeRightNode(CBType treeNode)
	{
		if(treeNode != null)
			return treeNode.right;
		else
			return null;
	}
	/*
	 * 判断空树，就是判断一个二叉树的结构是否为空，如果为空，则表示该二叉树结构中没有数据
	 */
	int TreeIsEmpty(CBType treeNode)
	{
		if(treeNode!=null)
			return 0;
		return 1;
	}
	
	/*
	 * 计算二叉树的深度
	 */
	int TreeDepth(CBType treeNode)
	{
		int depthLeft, depthRight;
		
		if(treeNode==null)
		{
			return 0;        //对于空树，深度为0
		}
		else
		{
			depthLeft = TreeDepth(treeNode.left);         //左子树递归
			depthRight = TreeDepth(treeNode.right);   //右子树递归
			
			if(depthLeft>depthRight)
				return depthLeft+1;
			return depthRight+1;
		}
	}
	/*
	 * 清空二叉树
	 */
	void ClearTree(CBType treeNode)
	{
		if(treeNode != null)
		{
			ClearTree(treeNode.left);           //清空左子树
			ClearTree(treeNode.right);            //清空右子树
			treeNode = null;
		}
	}
	
	/*
	 * 显示节点数据
	 */
	void TreeNodeData(CBType p)
	{
		System.out.print(p.data + "\t");
	}
	
	/*
	 * 二叉树的三种遍历方式
	 */
	//先序遍历
	void DLRTree(CBType treeNode)
	{
	if(treeNode!=null)
		{
			TreeNodeData(treeNode); //显示节点数据
			DLRTree(treeNode.left);
			DLRTree(treeNode.right);
		}
	}
	//中序遍历
	void LDRTree(CBType treeNode)
	{
		if(treeNode != null)
		{
			LDRTree(treeNode.left);     //中序遍历左子树
			TreeNodeData(treeNode); //显示节点数据
			LDRTree(treeNode.right);     //中序遍历左子树
		}
	}
	
	//后序遍历
	void LRDTree(CBType treeNode)
	{
		if(treeNode!= null)
		{
			LRDTree(treeNode.left);     //中序遍历左子树
			LRDTree(treeNode.right);     //中序遍历左子树
			TreeNodeData(treeNode); //显示节点数据	
		}
	}
	
	
	public static void main(String[] args) {
		CBType root = null;
		Scanner in = new Scanner(System.in);
		int menusel;          //决定添加到左子树还是右子树
		tree t= new tree();
		root = t.initTree();
		//添加节点
		do{
			System.out.println("请选择菜单添加二叉树的节点：");
			System.out.print("0：退出" + "\t");     //显示菜单
			System.out.println("1：添加二叉树的节点");
			menusel = in.nextInt();
			switch(menusel)
			{
			case 1:
				t.addTreeNode(root);     //添加节点
				break;
			case 0:
				break;
			default:
					;
			}
		}while(menusel!=0);
		
		//遍历
		do{
			System.out.println("请选择菜单遍历二叉树，输入0 表示退出");
			System.out.print("1：先序遍历DLR \t");
			System.out.print("2：中序遍历LDR \t");
			System.out.println("3：后序遍历LRD");
			menusel = in.nextInt();
			switch(menusel)
			{
			case 0:
				break;
			case 1:
				System.out.println("先序遍历DLR的结果:");
				t.DLRTree(root);
				System.out.println();
				break;
			case 2:
				System.out.println("中序遍历的结果：");
				t.LDRTree(root);
				System.out.println();
				break;
			case 3:
				System.out.println("后序遍历的结果：");
				t.LRDTree(root);
				System.out.println();
				break;
			default:
				;
			}
		}while(menusel != 0);
			//树的深度
			System.out.println("\n二叉树的深度为：" + t.TreeDepth(root));
			
			//清空二叉树
			t.ClearTree(root);
			root = null;
	}
	
}