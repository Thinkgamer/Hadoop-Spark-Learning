package Link;

import java.util.Scanner;

public class DoubleLinkTest {

	public static void main(String[] args) {
		
		DoubleLink node=null, head=null;
		DoubleLink dlink = new DoubleLink(); //声明一个双向链表对象
		Scanner scan  = new Scanner(System.in);
		
		System.out.println("双向链表测试开始....");
		do{
			System.out.println("请输入插入节点的关键字，姓名和年龄，格式为：关键字	姓名	年龄");
			Data data = new Data();
			data.key = scan.next();
			data.name = scan.next();
			data.age = scan.nextInt();

			if(data.key.contains("0"))  //循环插入节点，直到插入的为0时结束
				break;
			else
			{
				System.out.println("请输入插入节点的前驱节点和后继节点，格式为 前驱节点  后继节点");
				String priorKey = scan.next();
				String nextKey = scan.next();
				
				head = dlink.addNode(head, priorKey, nextKey, data);   //添加节点 
				dlink.OutputLinkNode(head);   //输出链表
			}
		}while(true);
		
		//输出链表的深度
		System.out.println("该链表的深度为：" + dlink.LinkDepth(head));
		
		//查找链表中的某个节点
		System.out.println("请输入要查找的节点的关键字...");
		String findKey = scan.next();
		node = dlink.FindLink(head, findKey);
		 if(node==null)
			 System.out.println("你所查找的节点不存在！");
		 else
			 System.out.println("该节点的值为：" + node.data.key + "\t" + node.data.name + "\t" + node.data.age);
		
		 //删除节点值
		 System.out.println("请输入要删除的节点的关键字...");
		 String deleteKey = scan.next();
		 node =  dlink.DeleteNode(head, deleteKey);
		 if(node == null)
			 System.out.println("删除节点后的链表为空，其深度为：" + 0);
		 else
		 	{
				 System.out.println("删除后的链表为：");
				 dlink.OutputLinkNode(head);
				 System.out.println("删除节点后链表的深度为：" + dlink.LinkDepth(head));
		 	}
	}
}
