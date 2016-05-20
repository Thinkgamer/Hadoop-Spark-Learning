package Queue;

/*
 * 使用java构建队列，并模拟实现队列的入队和出对方法
 */

public class Queue {     //队列类

	private int maxSize;  //定义队列的长度
	private int[] arrQueue;      //队列
	private int rear;     //定义队列的尾指针
	private int front;   //定义队列的头指针
	private int empty;  //元素的个数
	
	public Queue(int s)   //初始化构造函数
	{
		maxSize = s;
		arrQueue = new int[s];
		rear = -1;
		front=0;
		empty = 0;
	}
	
	//实现插入方法
	public void insert(int m)
	{
		if(rear == maxSize-1)   //处理循环
			rear = -1;      
		arrQueue[++rear] = m;   //对尾指针加一，把值放在队列结尾
		empty++;      //队列元素个数加1
		System.out.println("队列入队元素 为：" + m);
	}
	
	//实现出栈的方法，即取得队列的头元素
	public int remove()
	{
		int temp = arrQueue[front++]; //将栈顶元素赋值给temp，栈顶指针加1
		if(front == maxSize) //处理循环
			front = 0;
		empty--; //元素个数-1
		return temp;
	}
	
	//判断队列是否为空
	public boolean isEmpty()
	{
		return (empty==0);
	}
	
	//判断对列是否为满
	public boolean isFull()
	{
		return (empty == maxSize);
	}
	
	//返回队列长度
	public int qLong()
	{
		return empty;
	}
	
	public static void main(String[] args) {
		Queue q = new Queue(5); //初始化队列为5个元素
		
		q.insert(1);
		q.insert(2);
		q.insert(3);
		q.insert(4);
		q.insert(5);
		
		int t1 = q.remove();
		System.out.println("队列元素出队：" + t1);
		int t2 = q.remove();
		System.out.println("队列元素出队：" + t2);
		
		System.out.println("队列是否为空：" + q.isEmpty());
		System.out.println("队列是否为满：" + q.isFull());
		System.out.println("队列的长度：" + q.qLong());
	}
	
}
