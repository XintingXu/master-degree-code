# master-degree-paper
北京理工大学，计算机学院，徐欣廷，硕士毕业论文，实验部分需要的所有代码。

## 组成部分
项目主要由配置文件、共享头、各部分具体实现组成。

### 配置文件 ./CONFIG
所有的配置项均以json格式保存，包含数据库信息，调制、解调的参数信息。

#### A. database.json
1. 数据库地址、用户名、口令
2. 数据库名

#### B. covertmessage.json
生成隐蔽消息
1. 负载消息数据表
2. 生成负载的数量
3. 每条消息的长度

#### C. generatenoise.json
生成随机噪声
1. 噪声数据表
2. 随机噪声类型（默认为2）
3. 平均丢包率的比例
4. 每种丢包率条目数量
5. 每个项的最大数据包长度

#### D. parameters.json
实验参数配置
1. 实验需要的参数配置表
2. 实验方案类型（1表示方案一，2表示方案二），不同方案需要的参数类型不同
3. 各参数候选项，根据不同方案，只选择有效的参数组合

#### E. modulation.json
调制及解调过程公用
1. 调制数据库及原始信息表的定义
2. 操作对象的类型，1表示原始的抓包，2表示生成的随机噪声，3表示细分场景
3. 并行线程数
4. 是否开启DEBUG调试，0不开启，1开启。开启后强制单线程，并将中间结果记录到log文件
5. 加盐噪声，包括HASH SALT及OFFSET SALT，主要用于方案二

#### F. distribution.json
统计参数配置
1. 数据库及数据表信息
2. 数据类型，只允许1或3
3. 并行线程数
4. 滑动窗口的起始、结束及步长

### 公用文件头 ./include
所有项目公用的头文件，及部分方法的实现

#### 参数解析
解析json配置文件，并按照数据库参数结构体及参数映射表返回

#### 进程执行逻辑
执行各阶段的初始化、清理操作

### 1-Covert-Message-Generation
生成用于发送的Covert Message

### 2-Parameters-Combination-Generation
生成实验需要的参数组合

### 3-Generate-Random-Noise
生成随机噪声

### 4-Modulation-Scheme-1
第一种方案的调制过程

### 4-Demodulation-Scheme-1
第一种方案的解调过程

### 4-Modulation-Scheme-2
第二种方案的调制过程

### 4-Demodulation-Scheme-2
第二种方案的解调过程

### 5-Calculation-Distribution
根据统计设定，统计各分部的统计值、概率、累积分布

### 5-Calculation-Distribution-Quantification-Python
Python项目，对调制前及调制后的分布，进行一致性假设检验、熵检验及距离检验
