package Link;

import java.util.Scanner;

public class linkTest {

	public static void main(String[] args) {
		Link node = null , head=null;
		Link link = new Link();
		String key, findKey;
		Scanner input = new Scanner(System.in);
		
		System.out.printf("链表测试开始，先输出链表中的数据，格式为：关键字	姓名	年龄\n");
		do
		{                        //循环插入节点，知道输入的key 为0 结束
			DATA nodeData = new DATA();
			nodeData.key = input.next();
			if(nodeData.key.equals("0"))
			{
				break;
			}
			else
			{
				nodeData.name = input.next();
				nodeData.age = input.nextInt();
				head = link.linkAddEnd(head, nodeData);  //在链表尾部添加节点
			}
		}while(true);
		link.linkShow(head);     //显示所有节点
		
		System.out.printf("\n演示插入节点，输入插入位置的关键字：");
		findKey = input.next();                //输入插入的关键字
		System.out.println("输入插入节点的数据(关键字 姓名 年龄)");
		DATA nodeData = new DATA();              //输入节点的元素值
		nodeData.key = input.next();
		nodeData.name = input.next();
		nodeData.age = input.nextInt();
		head = link.linkInsertNode(head, findKey, nodeData);           //调用插入函数
		link.linkShow(head);    //显示所有节点
		
		System.out.println("演示删除节点，输入要删除的关键字：");
		key = input.next();
		link.linkDeleteNode(head, key);         //调用删除节点的函数
		link.linkShow(head);                     //显示所有节点
		
		System.out.println("演示在链表中差找，输入要查找的关键字：");
		key = input.next();
		node = link.linkFindNode(head, key);  //调用查找函数,返回节点引用
		if(node!=null)
		{
			nodeData = node.nodeData;         //获取节点的数据
			System.out.printf("关键字 %s 对应的节点数据为 （%s %s %s）\n", key,nodeData.key,nodeData.name,nodeData.age);
		}
		else
		{
			System.out.printf("在链表中为查找的为%s 的关键字 \n" , key);
		}
		
		
	}
	
}
