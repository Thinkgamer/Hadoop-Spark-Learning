package WebKPI;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashSet;
import java.util.Locale;
import java.util.Set;

public class KPIfilter {
	
	//自定义错误计数器，在执行完程序时显示相应的错误条数
	//由于web日志并不是规格的，存在部分数据不完整或者格式有问题，故设计计数
	private static int numUser_agent = 0; //用户代理
	private static int numStatus = 0; //访问状态码

	
	private String remote_ip;        //记录来源的ip地址，通过ip地址我们可以得到地域
	private String remote_time;  //记录访问的时间和时区
	private String request;            //记录请求方式
	private String status;               //网站请求状态码
	private String body_byte_sent;     //请求网页时反馈的字节大小
	private String see_url;            //表示从哪个页面连接过来
	private String user_agent;        //记录用户浏览的相关信息
	
	public int getNumUser_agent() {
		return numUser_agent;
	}

	public static int getNumStatus() {
		return numStatus;
	}
	
	private boolean valid = true;       //判断数据是否合法

	@Override
	public String toString() {
		// TODO Auto-generated method stub
		StringBuilder sb = new StringBuilder();
		sb.append("valid:" + this.valid);
		sb.append("\nremote_ip:" + this.remote_ip);
		sb.append("\nremote_time:" + this.remote_time);
		sb.append("\nrequest:" + this.request);
		sb.append("\nstatus:" + this.status);
		sb.append("\nbody_byte_sent:" + this.body_byte_sent);
		sb.append("\nsee_url:" + this.see_url);
		sb.append("\nuser_agent:" + this.user_agent);
		return sb.toString();
	}

	//get remote_ip
	public String getRemote_ip() {
		return remote_ip;
	}

	//set remote_ip
	public void setRemote_ip(String remote_ip) {
		this.remote_ip = remote_ip;
	}


	 public Date getTime_local_Date() throws ParseException {
	        SimpleDateFormat df = new SimpleDateFormat("dd/MMM/yyyy:HH:mm:ss", Locale.US);
	        return df.parse(this.remote_time);
	    }
	    
	    public String getTime_local_Date_hour() throws ParseException{
	        SimpleDateFormat df = new SimpleDateFormat("yyyyMMddHH");
	        return df.format(this.getTime_local_Date());
	    }

	//get remote_time
	public String getRemote_time() {
		return remote_time;
	}
	
	//set remote_time，时间转化为Unix时间戳
	public void setRemote_time(String remote_time) {
		this.remote_time = remote_time.substring(1);
	}

	//get request
	public String getRequest() {
		return request;
	}

	//set request
	public void setRequest(String request) {
		this.request = request.substring(1);
	}

	//get status
	public String getStatus() {
		return status;
	}
	//set status
	public void setStatus(String status) {
		this.status = status;
	}

	//get body_byte_sent
	public String getBody_byte_sent() {
		return body_byte_sent;
	}

	//set body_byte_sent
	public void setBody_byte_sent(String body_byte_sent) {
		this.body_byte_sent = body_byte_sent;
	}

	//get from_url
	public String getSee_url() {
		return see_url;
	}

	//set from_url
	public void setSee_url(String see_url) {
		this.see_url = see_url;
	}

	//get user_agent
	public String getUser_agent() {
		return user_agent;
	}

	//set user_agentl
	public void setUser_agent(String user_agent) {
		try{
			this.user_agent = user_agent.substring(1);
		}catch(Exception e){
//			e.printStackTrace();
			System.out.println("user_agent is inlegal");
			this.user_agent = "-";
			this.numUser_agent ++;
		}
	}

	//get valid
	public boolean isValid() {
		return valid;
	}

	//set valid
	public void setValid(boolean valid) {
		this.valid = valid;
	}
	
	//解析每行日志
	public static KPIfilter parser(String line) throws ParseException{
//		System.out.println(line);
		KPIfilter kpi = new KPIfilter();   //声明一个KPIfilter的对象
		
		String[] arr = line.split(" ");
		//日志数据并非是规则的，但最短长度为12,所以要大于11
		if(arr.length>11){
			
			try{
				kpi.setRemote_ip(arr[0]);       //设置IP
				kpi.setRemote_time(arr[3]); //设置时间
				kpi.setRequest(arr[5]);        //设置请求方式
				kpi.setStatus(arr[8]);            //设置返回的状态码
				kpi.setBody_byte_sent(arr[9]); //设置返回的字节数
				kpi.setSee_url(arr[6]);          //设置来源页面
				kpi.setUser_agent(arr[11]);           //设置请求信息
//				System.out.println(kpi);
				
	
	//	        SimpleDateFormat df = new SimpleDateFormat("yyyy.MM.dd:HH:mm:ss", Locale.US);
	//	        System.out.println(df.format(kpi.getTime_local_Date()));
	//	        System.out.println(kpi.getTime_local_Date_hour());
				try{
					if (Integer.parseInt(kpi.getStatus()) >= 400) {// 大于400，HTTP错误
		                kpi.setValid(false);
		            }
				}catch(Exception e){
//					e.printStackTrace();
					System.out.println("Status is error");
					kpi.setStatus(arr[9]);
					if (Integer.parseInt(kpi.getStatus()) >= 400) {// 大于400，HTTP错误
		                kpi.setValid(false);
		                numStatus++;
		            }
				}
//				
			}catch(Exception e){
//				e.printStackTrace();
				kpi.setValid(false);
			}
			
			
		}else{//如果长度小于12,则为不满足条件，设置valid为false
			kpi.setValid(false);
		}
		
		return kpi;
	}
	
	//按page的pv分类，过滤指定网页的浏览量
	public static KPIfilter filterPVs(String line) throws ParseException
	{
		KPIfilter kpi = parser(line);
		Set pages =new HashSet();
		
		pages.add("/213.238.172.248");
		pages.add("/order-form/");
		pages.add("/index.php");
		pages.add("http://www.addamiele.it/");
		pages.add("http://www.tianya.cn/ ");
		pages.add("http://www.google.com/");
		
		if(pages.contains(kpi.getSee_url()))
		{
			kpi.setValid(true);
		}else{
			kpi.setValid(false);
		}
		return kpi;
	}
	
	public static void main(String [] args) throws ParseException {
		String line = "31.3.245.106 - - [25/Apr/2016:06:55:21 +0800] \"CONNECT www.marathonbet.com:443 HTTP/1.1\" 405 575 \"https://www.marathonbet.com/en/live/26418\" \"Mozilla/5.0 (Windows NT 6.1; WOW64; rv:16.0) Gecko/20100101 Firefox/16.0";
		KPIfilter kpi = new KPIfilter();
		kpi = kpi.parser(line);		
		System.out.println(kpi.toString());
		System.out.println(kpi.getTime_local_Date_hour());
	}
	
}
