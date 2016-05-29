package Link;

class DATA{           //定义链表的一个节点
	String key;           //节点的关键字
	String name;
	int age;
}

public class Link {     //定义链表结构
	
	DATA nodeData = new DATA();     //声明一个节点
	Link nextNode;                                //指向下一个节点的指针
	
	
	//添加节点
	Link linkAddEnd(Link head, DATA nodeData)
	{
		Link node, hTemp;
		if( (node = new Link()) ==null)        //如果内存空间分配失败，则返回为空
		{
			System.out.println("内存空间分配失败！");
			return null;
		}
		else
		{
			node.nodeData = nodeData;
			node.nextNode = null;
			if(head == null)      //如果头节点为空，则把当前节点赋给head，并返回
			{
				head = node;
				return head;
			}      
			hTemp = head;       //如果头节点不为空
			while(hTemp.nextNode!=null)        //查找链表的末尾
			{
				hTemp = hTemp.nextNode;
			}
			hTemp.nextNode = node;
			return head;
		}
	}
	
	//插入头节点
	Link linkAddFirst(Link head, DATA nodeData)
	{
		Link node;
		if((node=new Link()) == null ) //如果内存空间分配失败，则返回为空
		{
			System.out.println("内存分配失败");
			return null;
		}
		else
		{
			node.nodeData = nodeData;
			node.nextNode = head;
			head = node;
			return head;
		}
	}
	
	//查找节点
	Link linkFindNode(Link head, String key)
	{
		Link hTemp;
		hTemp = head;
		while(hTemp!=null)       //若节点有效，则进行查找
		{
			if(hTemp.nodeData.key.compareTo(key) == 0) //若节点的关键字与传入的关键字相同
			{
				return hTemp;
			}
			hTemp = hTemp.nextNode;     //处理下一个节点
		}
		return null;	
	}
	
	//插入节点
	Link linkInsertNode(Link head, String findKey,DATA nodeData)
	{
		Link node,hTemp;
		if((node = new Link() ) == null ) //分配内存失败，则返回
		{
			System.out.println("分配内存失败...");
			return null;
		}
		node.nodeData = nodeData;      //保存当前集节点信息
		hTemp = linkFindNode(head, findKey);      //查找要插入的节点
		if(hTemp != null)
		{
			node.nextNode = hTemp.nextNode;
			hTemp.nextNode = node;
		}
		else
		{
			System.out.println("未找到正确的插入位置.........");
		}
		return head;          //返回头引用
	}
	
	//删除节点
	int linkDeleteNode(Link head, String key)
	{
		Link node,hTemp;
		hTemp = head;
		node = head;
		while(hTemp != null )
		{
			if(hTemp.nodeData.key.compareTo(key) == 0)   //若找到关键字，则删除
			{
				node.nextNode = hTemp.nextNode;
				hTemp = null;
				return 1;
			}
			else               //跳到下一个节点
			{
				node = hTemp;
				hTemp = hTemp.nextNode;
			}
		}
		return 0;
	}
	
	//计算链表长度
	int linkLength(Link head)
	{
		Link hTemp;
		hTemp = head;
		int num = 0;
		while(hTemp!=null)
		{
			num ++ ;
			hTemp = hTemp.nextNode;
		}
		return num;	
	}
	
	//显示所有节点
	void linkShow(Link head)
	{
		Link hTemp;
		DATA nodeData;
		hTemp = head;
		System.out.printf("当前链表共有 %d 个节点，链表所有的数据如下：\n" , linkLength(head));
		while(hTemp!=null)
		{
			nodeData = hTemp.nodeData;     //获取当前的节点数据
			System.out.printf("节点(%s %s  %d)\n",nodeData.key,nodeData.name,nodeData.age);
			hTemp = hTemp.nextNode;
		}
	}
	
}