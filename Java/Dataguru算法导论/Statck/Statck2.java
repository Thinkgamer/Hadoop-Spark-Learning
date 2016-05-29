package Statck;

import java.util.ArrayList;
import java.util.EmptyStackException;
import java.util.List;

/*
 * 使用java构建栈，并模拟实现栈的入栈和出栈方法
 * 使用链表实现
 */

public class Statck2<E extends Object> {  
	
    private List<E> statck = new ArrayList<E>(); 
	
	public Statck2(){
		      //栈的初始化
	}
	
	//清空栈
	public void clear(){
		statck.clear();
		System.out.println("清空栈..........");
	}
	//判断栈是否为空
	public boolean isEmpty(){
		return statck.isEmpty();
	}
	//获取栈顶元素
	public E getTop(){
		if(isEmpty())
			return null;
		return statck.get(0);
	}
	
	//弹出栈操作
	public E pop(){
		if (isEmpty()) 
			throw new EmptyStackException();  
		System.out.println(statck.size() + "\t 出栈");
        return statck.remove(statck.size() - 1);  
	}
	
	//压入栈操作
	public void push(E e){
		statck.add(e);
		System.out.println(e + "\t 入栈");
	}
	
	//获取当前栈的深度
	public int getStatckSize(){
		if(isEmpty())
			throw new EmptyStackException();
		return statck.size();
	}
	
	public static void main(String[] args) {
		Statck2 s = new Statck2();
		s.clear();           //清空栈
		System.out.println("当前栈是否为空：" + s.isEmpty());
		s.push(1);
		s.push(2);
		s.push(3);
		
		s.pop();
		System.out.println("当前栈的深度为：" + s.getStatckSize());
		System.out.println("当前栈顶元素为：" + s.getTop());
	}
	
}
