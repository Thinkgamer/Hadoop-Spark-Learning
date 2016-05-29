package Hash;
/*
 * Hash函数介绍
 * 除下面介绍的集中hash函数外还有取余散列法（m一般选择较大的素数，例如701） h(k) = k mod m
 * 乘法散列法（m选择2的计算机的位数（64位或者32位）,A为sqrt(5)-1 = 0.618）h(k) = m(kA mod 1 )
 */

public class hash {

	//1：RS
    public static long RSHash(String str)  
    {  
       int b     = 378551;  
       int a     = 63689;  
       long hash = 0;  
       for(int i = 0; i < str.length(); i++)  
       {  
          hash = hash * a + str.charAt(i);  
          a    = a * b;  
       }  
       return hash;  
    }  
    
    //2：JS  Justin Sobel写的一个位操作的哈希函数。
    public static long JSHash(String str)  
       {  
          long hash = 1315423911;  
          for(int i = 0; i < str.length(); i++)  
          {  
             hash ^= ((hash << 5) + str.charAt(i) + (hash >> 2));  
          }  
          return hash;  
       }  
	
    //3：PJW 	该散列算法是基于贝尔实验室的彼得J温伯格的的研究。在Compilers一书中（原则，技术和工具），建议采用这个算法的散列函数的哈希方法。 
    public static long PJWHash(String str)  
    {  
        long BitsInUnsignedInt = (long)(4 * 8);  
        long ThreeQuarters     = (long)((BitsInUnsignedInt  * 3) / 4);  
        long OneEighth         = (long)(BitsInUnsignedInt / 8);  
        long HighBits          = (long)(0xFFFFFFFF) << (BitsInUnsignedInt - OneEighth);  
        long hash              = 0;  
        long test              = 0;  
        for(int i = 0; i < str.length(); i++)  
        {  
           hash = (hash << OneEighth) + str.charAt(i);  
           if((test = hash & HighBits)  != 0)  
           {  
              hash = (( hash ^ (test >> ThreeQuarters)) & (~HighBits));  
           }  
        }  
        return hash;  
     } 
    
    //4：ELF      和PJW很相似，在Unix系统中使用的较多。
    public static long ELFHash(String str)  
       {  
          long hash = 0;  
          long x    = 0;  
          for(int i = 0; i < str.length(); i++)  
          {  
             hash = (hash << 4) + str.charAt(i);  
             if((x = hash & 0xF0000000L) != 0)  
             {  
                hash ^= (x >> 24);  
             }  
             hash &= ~x;  
          }  
          return hash;  
       }  
    
    //5：BKDR
    /*
     * 这个算法来自Brian Kernighan 和 Dennis Ritchie的 The C Programming Language。
     * 这是一个很简单的哈希算法,使用了一系列奇怪的数字,形式如31,3131,31...31,看上去和DJB算法很相似
     */
    public static long BKDRHash(String str)  
       {  
          long seed = 131; // 31 131 1313 13131 131313 etc..  
          long hash = 0;  
          for(int i = 0; i < str.length(); i++)  
          {  
             hash = (hash * seed) + str.charAt(i);  
          }  
          return hash;  
       }  
    
    //6：SDBM     这个算法在开源的SDBM中使用，似乎对很多不同类型的数据都能得到不错的分布。
    public static long SDBMHash(String str)  
    {  
       long hash = 0;  
       for(int i = 0; i < str.length(); i++)  
       {  
          hash = str.charAt(i) + (hash << 6) + (hash << 16) - hash;  
       }  
       return hash;  
    }
    
    //7：DJB 这个算法是Daniel J.Bernstein 教授发明的，是目前公布的最有效的哈希函数
    public static long DJBHash(String str)  
       {  
          long hash = 5381;  
          for(int i = 0; i < str.length(); i++)  
          {  
             hash = ((hash << 5) + hash) + str.charAt(i);  
          }  
          return hash;  
       }  
    
    //8：DEK    由伟大的Knuth在《编程的艺术 第三卷》的第六章排序和搜索中给出。
    public static long DEKHash(String str)  
       {  
          long hash = str.length();  
          for(int i = 0; i < str.length(); i++)  
          {  
             hash = ((hash << 5) ^ (hash >> 27)) ^ str.charAt(i);  
          }  
          return hash;  
       }  
    
    //9：AP    这是本文作者Arash Partow贡献的一个哈希函数，继承了上面以旋转以为和加操作。代数描述：AP
    public static long APHash(String str)  
       {  
          long hash = 0xAAAAAAAA;  
          for(int i = 0; i < str.length(); i++)  
          {  
             if ((i & 1) == 0)  
             {  
                hash ^= ((hash << 7) ^ str.charAt(i) * (hash >> 3));  
             }  
             else  
             {  
                hash ^= (~((hash << 11) + str.charAt(i) ^ (hash >> 5)));  
             }  
          }  
          return hash;  
       }  
    
   //主函数
	public static void main(String[] args) {
		String str = "thinkgamer";
		System.out.println("thinkgamer 的 RSHash：" + RSHash(str));
		System.out.println("thinkgamer 的  JSHash：" + JSHash(str));
		System.out.println("thinkgamer 的 PJWHash：" + PJWHash(str));
		System.out.println("thinkgamer 的 ELFHash：" + ELFHash(str));
		System.out.println("thinkgamer 的 BKDRHash：" + BKDRHash(str));
		System.out.println("thinkgamer 的 SDBMHash：" + SDBMHash(str));
		System.out.println("thinkgamer 的 DJBHash：" + DJBHash(str));
		System.out.println("thinkgamer 的 DEKHash：" + DEKHash(str));
		System.out.println("thinkgamer 的 APHash：" + APHash(str));
	}
}
