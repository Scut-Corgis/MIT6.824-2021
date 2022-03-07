**学习MIT6.824过程中做的课程lab**

我看的是2020Spring的课程视频，但是此lab是2021Spring的。

目前做完了全部四个LAB。

**文件夹分布**

LAB1 ： MapReduce

* main	mr	mrapp	(mr为Lab任务)

LAB2: 	Raft

* raft

LAB3:	kvRaft

* kvraft

LAB4:	shardKV Server

* shardctrler (协调者，分片控制者)	shardkv (分片数据库部分)

**20220127**

LAB1 和 LAB2 均100次测试无一FAIL。

LAB3 100次全部测试，出现了4次Fail，其中3次均为少append了仅一个数据，1次数据线性化失败。

因为这段时间debug过于劳累，这种少数情况出现的bug需要大量的精力在几十万条日志中排查，等有时间再解决它。

```shell
cat 100次lab3全部测试结果.log | grep -n "FAIL"
2321:--- FAIL: TestSnapshotUnreliableRecoverConcurrentPartitionLinearizable3B (38.07s)
2323:FAIL
2325:FAIL       6.824/kvraft    444.007s
2375:--- FAIL: TestSnapshotUnreliableRecoverConcurrentPartition3B (28.57s)
2383:FAIL
2385:FAIL       6.824/kvraft    442.443s
3353:--- FAIL: TestSnapshotUnreliableRecoverConcurrentPartition3B (19.77s)
3366:FAIL
3368:FAIL       6.824/kvraft    437.729s
4120:--- FAIL: TestSnapshotUnreliableRecoverConcurrentPartition3B (19.11s)
4129:FAIL
4131:FAIL       6.824/kvraft    434.286s
```



**20220208**

LAB3 100次测试全部通过，BUG全部消除

LAB3 bug基本全部fix

重新做了100次测试，检查后全部通过。

但还是有其中一次小测试即不到百分之一的概率出现线性化错误，测试结果均在对应文件夹内。

此线性化错误我目前没有找到排查的办法，无从下手，因为是极其罕见的FAIL，留给有思路的时候排查。



**20220301**

完成LAB4，开始几乎没有思路，参考了很多博主的博客，逐步实现了一个LAB4分布式分片数据库。

感觉应该没有常态bug了，test包括challenge都能通过。

偶尔还是会出现一些测试fail的bug， 以后有空再fix吧，花太多时间在6.824上了，该进入下一阶段的学习了...

****

**测试结果均置于其文件夹下**

> src文件夹中含我的笔记(心得)



附课程链接: http://nil.csail.mit.edu/6.824/2021/labs/lab-kvraft.html



最后感谢MIT的课程组带来了这么好的课程和LAB，让我从一个分布式小白和go语言小白成长为了至少一个菜鸟了。

