# 集群说明：
三个节点：分别为master1和slave1，slave2（由于电脑资源有限）

master1 文件夹中为主节点的配置文件

slave1 文件夹中为从节点的配置文件（如果有多个从节点，可复制）

slave2 文件夹中为从节点2的配置文件

# hosts文件修改说明

在主从节点的/etc/hosts中加入以下两行或者多行

```
master1IP master1
slave1IP slave1
slave2IP slave2
```

# java options问题

关于deepin执行java -version显示
```
Picked up _JAVA_OPTIONS:   -Dawt.useSystemAAFontSettings=gasp
```
是正常的，对于强迫症的我，将其除去的办法是：

在 /etc/profile中加入
```
unset _JAVA_OPTIONS

```

---

# 遇到的问题及解决办法*
## 1： sign_and_send_pubkey: signing failed: agent refused operation
这是因为ssh 产生的秘钥没有加入到系统中，执行 ssh-add即可

## 2：Error: JAVA_HOME is not set and could not be found.
原因：我安装java环境的时候采用的deb安装的，所以系统已经有了$JAVA_HOME，但是在hadoop/etc/hadoop/hadoop-env.sh中不识别
>export JAVA_HOME=${JAVA_HOME}

这里将${JAVA_HOME}换成你自己的java环境路径即可，可以通过
>echo $JAVA_HOME

来查看

## 3：hadoop datanode 服务启动不成功
原因：datanode的clusterID 和 namenode的 clusterID 不匹配
解决办法：
根据 hdfs-site.xml 中的配置：
1、 打开 dfs.namenode.name.dir 配置对应目录下的 current 目录下的 VERSION 文件，拷贝clusterID；
2、 打开 dfs.datanode.data.dir 配置对应目录下的 current 目录下的 VERSION 文件，用拷贝的 clusterID 覆盖原有的clusterID；
3、 保存后重新启动 hadoop，datanode 进程就能正常启动了。

## 4：hive配置后启动错误
错误：Failed to get schema version
原因：在hive-site.xml配置javax.jdo.option.ConnectionURL value时，我把其中mysql所在的服务器的IP写成了用户名，这里改为localhost或者IP即可

## 5：从节点19888端口无法访问

执行：mr-jobhistory-daemon.sh start historyserver

---

# 运行MR在远程集群的两种办法
## 1：提交jar包
```
hadoop jar xxx.jar classname inputpath outputpath
```

## 2：代码中进行配置
```
Configuration conf = new Configuration();
conf.set("mapreduce.app-submission.cross-platform", "true");
conf.set("yarn.resourcemanager.address", "http://master1:8032");
conf.set("mapreduce.framework.name", "yarn");
Job job = Job.getInstance(conf, "wordcount");
```
