package Link;

import java.util.Scanner;

class Data{           //定义链表的一个节点
	String key;           //节点的关键字,唯一
	String name;
	int age;
}

public class DoubleLink {

	
	int flag; //输入选择值
	Scanner scan = new Scanner(System.in);
	Data data = new Data();
	DoubleLink nextNode;  //后继节点
	DoubleLink priorNode;    //前驱节点
	
	//链表添加节点
	DoubleLink addNode(DoubleLink head, String priorKey, String nextKey, Data nodeData){
		
		DoubleLink node=null, htemp=null;
		if((node = new DoubleLink()) == null)
			System.out.println("内存空间分配失败");
		if(head== null)        //如果head为空
		{
			System.out.println("当前链表为空，是否将当前节点当作头节点？\n0：否\t1：是");
			
			node.data=nodeData;
			node.nextNode=null;
			node.priorNode=null;
			flag = scan.nextInt();
			switch(flag)
			{
			case 0:
				break;
			case 1:
				head=node;
				break;
			default:
					System.out.println("你输入的数据不合法");;
			}
		}       //如果head不为空
		else{
			if(linkFindNode(head, priorKey,nextKey,nodeData))
				System.out.println("插入成功");
			else
				System.out.println("插入失败(原因可能是你输入的前驱和后继即诶但均不存在)");
		}
					
		return head;
	}

	//查找并插入节点
	boolean linkFindNode(DoubleLink head, String priorKey, String nextKey,Data nodeData) {
		// TODO Auto-generated method stub
		DoubleLink htemp=null,node=null;
		
		if( (node = new DoubleLink()) == null )
		{
			System.out.println("内存分配失败");
			return false;
		}
		//将传进来的值赋值给node
		node.data = nodeData;
		node.nextNode = null;
		node.priorNode=null;
		//两大类情况
		htemp = head;
		while(htemp != null)
		{
			if(htemp.data.key.equals(priorKey)) //前驱节点存在
			{
				if(htemp.nextNode == null)     //该节点的后继节点为空，说明该节点为头节点
				{
					System.out.println("你输入的后继节点不存在，前驱节点为头节点，是否插入在其后面？\n 1：是 \t 0 ：否 ");
					flag = scan.nextInt();
					if(flag == 0)
						break;
					else if(flag==1)
					{
						htemp.nextNode = node;       //将查找到的节点的后继节点指向node
						node.nextNode = null;
						node.priorNode = htemp;
						
						return true;
					}
					else
						System.out.println("你输入的数字不合法！！！");
				}
				else             //后继节点不为空
				{
					if(htemp.nextNode.data.key.equals(nextKey))            //存在的后继节点与nextKey相同。相同执行if
					{
						node.nextNode = htemp.nextNode;
						htemp.nextNode.priorNode = node;
						
						htemp.nextNode = node;
						node.priorNode = htemp;
						return true;
					
					}
					else         //不同执行else
					{
						htemp = htemp.nextNode; //若当前节点没找到，遍历下一个节点
					}
				}
			}
			else //前驱节点不存在，后驱节点存在
			{
				if(htemp.data.key.equals(nextKey))      //如果当前节点与nextKey相同
				{
					if(htemp.nextNode==null)  //如果后继节点为空，即当前节点为尾节点
					{
						System.out.println("你输入的前驱节点不存在，后继节点为头节点，是否插入在其前面？\n 1：是 \t 0 ：否 ");
						flag = scan.nextInt();
						if(flag == 0)
							break;
						else if(flag==1)
						{
							htemp.priorNode = node;
							node.nextNode = htemp;
							
							node.priorNode=null;
							return true;
						}
						else
							System.out.println("你输入的数字不合法！！！");
					}
					else //如果当前节点的后继节点不为空，则执行下一个节点
					{
						htemp = htemp.nextNode; //若当前节点没找到，遍历下一个节点
					}
				}
				else
					htemp = htemp.nextNode; //若当前节点没找到，遍历下一个节点
			}
		}
		return false;
	}
	
	//输出节点
	public void OutputLinkNode(DoubleLink head)
	{
		if(head == null)
			System.out.println("当前链表为空");
		else{
			System.out.println("输入的链表数据如下：");
			DoubleLink htemp;
			htemp = head;
			while(htemp!=null)
			{
				System.out.println(htemp.data.key + "\t" + htemp.data.name + "\t" + htemp.data.age);
				htemp= htemp.nextNode;
			}
		}
		System.out.println();
	}
	
	//输出链表的深度
	int LinkDepth(DoubleLink head)
	{
		int sum = 0;
		DoubleLink htemp = head;
		while(htemp!=null)
		{
			sum ++;
			htemp = htemp.nextNode;
		}
		return sum;
	}
	
	//查找节点
	DoubleLink FindLink(DoubleLink head, String findKey)
	{
		DoubleLink htemp=head;
		while(htemp!=null)
		{
			if(htemp.data.key.equals(findKey))
				return htemp;
			htemp = htemp.nextNode;
		}
		return null;
	}
	
	//删除节点
	DoubleLink DeleteNode(DoubleLink head, String deleteKey)
	{
		DoubleLink htemp = head;
		while(htemp!=null)
		{
			if(htemp.data.key.equals(deleteKey))
			{
				if(htemp.priorNode==null)  //如果是头节点
				{
					return htemp.nextNode;
				}
				else if (htemp.nextNode==null)     //如果是尾节点
				{
					htemp.priorNode.nextNode=null;
					htemp.priorNode=null;
					return head;
				}
				else //如果是中间
				{
					htemp.priorNode.nextNode=htemp.nextNode;
					htemp.nextNode.priorNode = htemp.priorNode;
					return head;
				}
			}
			else
				htemp = htemp.nextNode;
		}	
		System.out.println("你要删除的节点不存在！");
		return head;
	}
	
}
