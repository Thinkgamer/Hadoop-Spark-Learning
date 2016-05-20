package Statck;
/*
 * 使用java构建栈，并模拟实现栈的入栈和出栈方法
 * 使用数组实现
 */

public class Statck1 {

	private int maxSize;     //栈的最多元素数
	private int top;    //栈顶指针
	private int len;     //栈的深度
	private int[] arrStack; // 模拟栈
	
	//栈的初始化
	public Statck1(int s){
		maxSize = s;
		len =0;
		top= -1;
		arrStack = new int[s];
	}
	
	//获取栈的长度
	public int getLen(){
		return len;
	}
	
	//获取当前栈还能插入多少个f元素
	public int getLeaveLen(){
		return (maxSize-len);
	}
	//判断栈是否满
	public boolean isFull(){
		return (len==maxSize);
	}
	
	//判断栈是否为空
	public boolean isEmpty(){
		return (len ==0);
	}
	
	//元素入栈
	public void inStack(int s)
	{
		arrStack[++top] = s; //栈顶指针加1,入栈
		System.out.println("元素入栈：" + s);
		len ++ ;//栈深度+1
	}
	
	//元素出栈
	public int outStack()
	{
		int temp = arrStack[top--];//赋值之后减1
		System.out.println("元素出栈：" + temp);
		len--;   //栈深度-1
		return temp;
	}
	
	public static void main(String[] args) {
		Statck1 s = new Statck1(5);
		
		s.inStack(1);
		s.inStack(2);
		s.inStack(3);
		s.inStack(4);
		s.inStack(5);
		
		s.outStack();
		s.outStack();
		System.out.println("栈的长度：" + s.getLen());
		System.out.println("还能入栈元素个数：" + s.getLeaveLen());
		System.out.println("栈的是否为空：" + s.isEmpty());
		System.out.println("栈的是否为满：" + s.isFull());
	}
}
