# MIT-6.824-spring2021
MIT-6.824-spring2021, Raft using golang, includes lab 2A 2B 2C 2D 3A 3B 4A 4B    
 

## Recent
4A finished just now  
4B finished just now  
all lab has finished !! 
time to optimize codes...  

## Usage
### Lab2
cd src/raft  
  
#Single round test  
go test -run=2A -race  
  
#100 round test with up to 2 test programs running concurrently  
python3 dtest.py -n 100 -p 2 2D  

### Lab3
cd src/kvraft   

### Lab4A
cd src/shardctrler  

### Lab4B
cd src/shardkv  

## Correctness
Lab 2  : 10k rounds    
Lab 3  : 1k rounds 
Lab 4a : 10k rounds
Lab 4b : 100 rounds (takes much more time and cpu resouces than other tests)  



## Enable Logger
The logger can take full control of log printing using log topics  

We have prevent some logs from printing  
If you want to enable it  
Go to src/raft-logs/log-common.go   
Inside function init()   
Add log topics to print_list  
Also you can add more logTopic definitions as you like
```  
print_list := []LogTopic{
		//Clerk,
		//ServerApply,
		//Leader,
}
```  

## Any other question just email me 1291463831@qq.com

## Raft

![image-20210517164909985](../.resource/image-20210517164909985.png)

### 消息过滤

每次收到消息，加锁，忽略或者拒绝比自己小的term的消息

遇到比自己大的消息，toFollower 然后刷新voted、term。视为成功回复。

如果非follower状态，提前结束所处的函数，释放锁

同term过时的vote 请求与回复，直接过滤掉

同term的过时的append 请求， 回复用matchIndex过滤一下

同term的过时的install 请求，回复用matchIndex过滤一下

### 有效消息的处理

#### **vote**

不需要判断自己身份 leader、candidate（必然已经投给自己），投票成功时刷新超时！！！，根据votedFor、末尾日志比较投票。成为candidate时不需要给自己发voteRPC，直接记录成功

对于candidate：

检查身份，加锁判断term。数票数，如果成功则beNewLeader，失败足够多则重试

#### **append** 

小term拒绝（Xterm设为-2）。一定变为follower，需要对日志进行处理，更新commitIndex。

日志处理规则**快速回退**:

1. 如果previous index term都存在且match，则一定成功，从第一个不match的地方删除后续，如果match则不用删除。如果删除或者未删除后没有leader消息长，则append。设置nextIndex为previous +1 +消息中日志长度。返回true

2. 如果没有match，则如果是不存在则返回XTerm = -1，XLen=自身长度。leader会下次匹配Xlen长度（下次也可能失败）。返回false

3. 如果是存在但term不match。Xterm为follower不match的那个term，Xindex是该term自身的第一个index，Xlen为自身日志长度，返回false

对于leader：

检查身份，如果term小的直接拒绝，term大的自身变follower，提前返回。如果term相同（可能是过时的）

1. true 则okAsLeader更新nextIndex（优化：matchIndex小于nextIndex时），条件更新matchIndex、commitIndex、apply
2. false 且Xterm == -2说明是被拒绝的，忽略
3. false 且Xterm == -1说明太短，更新nextIndex 为Xlen（同上优化）
4. 如果leader含有Xterm日志，nextIndex更新为该term的最后一个日志。否则更新为Xindex

注意只要term足够大appendRpc总会更新心跳



### **Ticker**

检查自身身份，为follower时检查超时，超时进入newElection

### 其他

#### **发送同步rpc**

需要注意发送位置与结束位置，发送位置为nextIndex，结束位置可任意。如果nextIndex太大则发送空消息即可。如果太小，<= offset，则改为installSnapshot调用

#### **Commit**

成功收到appendRpc的回复success时。判断身份term，调用AppendOkAsLeader，可以更新nextIndex，可以试图更新matchIndex（单增）。如果成功更新matchIndex，可以统计major_match，并尝试更新commitIndex。

尝试更新lastApplied，给applier发送信号唤醒（发送cond.Signal)

#### 持久化数据

votedFor、currentTerm、logs、LastLogIndex（可不需要）、offset（snapshot用，表示snapshot最后保存的日志index，即lastIncludeIndex）。logs[0]保存一个哨兵，lastIncludeTerm（snapshot）

每次修改时persist一次

修改处：

currentTerm：大term、follower、candidate超时

votedFor：投票成功、candidate选自己、新term刷新掉

logs、lastLogIndex：appendLogs、deleteTailLogs、snapshot中操作

offset、lastIncludeTerm：应用snapshot时

#### NextIndex MatchIndex

总是可以安全地增大next Index，缩小nextIndex需要慎重，必须保证nextIndex大于matchIndex

matchIndex必须单增

滞后的append ok消息不应该使nextIndex回退到小于等于matchIndex

滞后的冲突消息，不应该使nextIndex回退到小于等于matchIndex

### snapshot

raft接口snapshot由service调用，传入snapshot数据。更改snap state、raft state

InstallSnapshot由peer调用，传入snapshot数据，判断是否过时，转交给apply chan即可

CondInstallSnapshot由service调用，传入snapshot，如果最近apply的不比snapshot新，则应用该snapshot类似snapshot接口、更改snap raft state然后返回true。反之则返回false即可。

返回true时service会安装该snapshot，false则忽略

**snapshot接口：**

无需判断身份、snapshot版本，加锁直接保存snap state，更新offset、log0，log长度与结构

**condInstallSnapshot**

加锁，判断lastApplied 与 snapshot的lastIncludeIndex大小，如果大于等于则拒绝返回false解锁

如果小于，确保先安装snapshot再解锁返回true

**installSnapshot**

为rpc，在log不够时调用。类似append，只不过logs换成snapshot，previous改为last

可以分段传送，上一段传完true再传下一段，自动重试

收到后：

如果不是期望段，则false，要求对方重试。接受完，判断index即可，无需判断term。如果include index比自己的last log index 大，即可转交给apply chan。更新nextIndex为 lastInclude + 1

**Server端**

在存储不够时调用raft的snapshot接口。自身除了创建snapshot调用接口外，不需要做任何操作

service在apply chan遇到true则确保先安装snapshot再处理后续，false不用管

初始化时从raft的snap state中初始化

自动拒绝小于applied index的apply请求

### 细节与踩坑

#### 技巧：

logs[0]做哨兵节点，存储term，记得及时清空其command以节省空间

timer 可以单独一个timerLock锁

可用cond唤醒applier，applier无需周期性检查

可用cond或者心跳唤醒leader，而不是每次start都发一次append

leader更新commitIndex 时视自身的matchIndex=lastIndex，平时就可以不用更新自己的matchIndex

leader在有日志发送时发送后sleep短时间，在无日志发心跳时sleep长时间

做snapshot与删除日志时，开新slice，消除无用引用，使得其可以垃圾回收

#### 踩坑：

刷新超时时机：收到>=currentTerm的 append或者installSnapshot请求，投票成功时。

apply chan死锁问题，引入applier，放锁后再写入applyChan，可在snapshot时避免死锁

lastApplied commited保持单增，matchIndex 再单个任期内单增，新term时归零

done的chan要开得足够大，timer关闭后要唤醒所有goroutine使其能够关闭

append成功后follower的commitIndex需要 <= lastLogIndex之前

## Lab3 Server

### client端

client生成时随机获取一个不重复的clientID

client任何请求，参数中带上clientID 与 消息序号seq

client做某个请求时，单线程循环重试，直到返回的消息中err为ok。ok后增大自身seq

### server端

#### 处理client请求

1. server端接受请求start拿到index（为leader时），先检查是否已经完成过该请求

2. 然后创建并等待在该index的channel上（所有channel都存在一个map内）

3. server的applier在apply该index的command后，会更新kv map，会更新对应的client的next_seq，会唤醒该channel。

4. 请求被唤醒后，检查该请求是否完成，返回结果给client

5. 请求完成的标志是server内保存的该client的next_seq足够大

#### applier

applier线程负责读取applyChan，并根据读出的消息apply

会过滤掉index小于lastApplied的消息，遇到不连续的较大的index时 panic

1. command则applyCommand，更新client对应的next_seq，更新kv map（如果已经执行过了该seq的命令，会跳过，这里实现了幂等）。判断是否需要snapshot

2. installSnapshot 则调用并根据condInstallSnap的结果选择是否apply该snapshot

更新lastApplied，唤醒等待在index channel上的请求(这里关闭该channel即可，等待的goroutine会读到零值)

#### snapshot

server的所有持久化数据都保存在snapshot内，这里有lastApplied，kv map，next_seq map三个数据

在apply snapshot时，更新kv next_seq两个map，lastApplied即可

在apply command后判断是否需要为raft生成snapshot



## Lab 4A ShardCtrler

实现一个主控集群，专门负责查询、更新config  (shard 在group间的分配)

client端与lab3一致，带上clientID seq，循环重试即可

### **server端**

定义command四种操作query、join、leave、move

RPC四个接口，与lab3也没有区别，负责将command通过Start 送进raft ,返回结果给client

#### apply command

同样的通过next_seq做**幂等**，判断是否已经执行过该命令，如果是则跳过，query操作也跳过

否则，通过balance算法算出最佳分配config，添加到configs数组即可

如果是move不需要balance，直接算出new config

必须在apply时才能计算new config，而不是在开始处理client请求时

#### balance 算法

map遍历顺序是随机的，不同server上遍历的顺序可能不一样，所以需要统一顺序

这里采用排序算法，在其他值相等的情况下，按group的gid排序，保证了一致性

要求达到最佳均衡，即不同group间分配到的shard数量最大差值不能超过1

注意可能有的group一个shard都没分到，可能所有group一个shard都没有

shard数组的元素的值表示分配给哪个组，其中0表示未分配给任何组

采用算法思想：

视shard分给0为free shard，可以被分给任何组

先列出各个group的id，join则把新groups算上，leave则去掉一些groups(其shard应该free掉)

用cnt_map统计各个group所含的shards数量，新join的为0，leave的group不在cnt_map内

排序group id，先按照所含shard数量（数量相同时按gid排序）

计算最优分配数量 必然是形式（avg, avg, ... ,avg+1, avg+1, avg+1)

对于对应位置超出最优的，free掉对应数量

对于对应位置少于最优的，则用free shard填充即可

#### Snapshot

需要保存的是configs、lastApplied、next_seq map

## Lab 4B ShardKV

config由主控集群shardCtrler负责，无需修改

### client端

基本无需修改，只需要请求时带上clientID 与seq即可，另外带上自己的config的index (num)

记得在client初始化前先查一次config

### Server端

pending shards是一个长度为Nshards的bool数组，代表由上一个config切换到当前config下，有哪些shard正在处理中（需要由本group发送给其他group，或者等待其他group发送给本group）

如果pendingShards[5] = true则代表shard 5还在处理中



额外定义两个goroutine

```go
go kv.nextConfigChecker()
go kv.shardSender()
```

checker每100ms负责检查下一个config，如果存在则发起new config propose（不需要等待结果）



sender 负责检查是否有待发送的pending shards，如果有，则发送给该group。

收到发来的shard、成功发送shard后的双方，都各自在自己的group内发起migration propose（让本group确认某个shard不再为pending状态）。

如果没有待发送的直接sleep即可

#### 处理client请求

同lab3，但先检查该请求的config是否与server的config的index一致，不一致则拒绝。

再检查当前config server是否对请求的key所在的shard负责，该shard是否处于可用状态。

之后再检查是否已经处理过该命令

start该command到raft中

然后等待结果

唤醒后，按上述过程再次检查，返回结果即可

#### Applier

这里apply command按类型分为apply config（对应propose new config）、apply migration（对应propose migration）、apply request（对应client的get put append请求）

**apply config**需要检查config是否已经apply过，是否是下一个config，是否当前已经没有pending shard了，必须如此才能apply。成功apply后，需要重新计算新的config的pending shards，更新pending shards

**apply migration**，也需要判断是否是当前config的，是否已经做过了，然后如果该shard是发送给别人的，就可以删除该shard，如果是别人发送给自己的，就更新该shard的数据。最后更新pending shard

**apply command**，也需要判断是否是当前config的请求，是否shard可用，是否已经apply过了。



上述机制保证了相同内容不会被多次重复操作，保证了幂等。

保证必须pending shard全部完成，才会进入下一个config。

保证了应用config后，某些shard能立即失效。保证收到某些shard并apply migration后，该shard能立即生效。保证group在某个index时点，对于某个client请求具有统一的行为

#### snapshot

snapshot持久化内容：lastIndex 、config、states(shards 包含kv与seq map)、pengding shards的bool数组

state为 shard数组，每个shard有自己的kv与seq map



修改持久化数据的唯一地方是apply，这保证了一致性与持久性

其中pengding shards会被apply new config的init pending，以及apply migration修改

shard会被apply migration、init pending（丢弃或者新增）改变，会被apply put append改变内容

config会被apply new config改变

lastIndex会被apply snapshot改变



### 机理

通过pending shards得bool数组持久化，将config的shard迁移过程持久化。同一config在处理shard迁移中途crash，重启后，不需要再重复迁移。

apply new config后，pending shards被改变。client请求时先检查config然后检查pending shards，保证了配置立即生效，失效shard立即失效。在migration 接收成功后，接收的该shard立即生效。

对方group确认接收成功后，shard可立即删除。

但目前组内所有成员均向对方发送RPC请求，可能会产生不必要的浪费。可以考虑错开时间发送

### Tips

允许在还存在pending shards时发起new config propose，但此propose在apply时必须没有pending shards才会apply成功，所以无需担心。

此设计是因为可能某个migration porpose得log已经被raft模块chosen，leader apply了并且告诉发送方已经成功了，但某个follower仅仅chosen还没有来得及apply。此时group crash，之前的follower仍可能成为新的leader，此时它没有删除pending shards，导致不能为外界提供服务，也导致config无法推进。

如果此时其他group因为发送成功而删除了该shard，且升级到了新的config，就不会给之前的group继续发送shard了，之前的group迟迟无法更新config，从而活锁。

解决办法很简单，只需要让该group认识到自己完全已经能够删除pending shards即可，换言之让该chosen的log变为apply即可。只需要向该leader的raft发送一次任意请求即可(leader只能commit自己任期内的log)。

外界的client请求可能会被config index或者shard不符合当前config而直接拒绝掉，不会发起start。但new config propose则可以直接且无害、周期性地start，是完美的选择











