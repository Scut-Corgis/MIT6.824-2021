**学习MIT6.824过程中做的课程lab**

我看的是2020Spring的视频，但是此lab是2021Spring的。

目前做完了前三个lab。

LAB1 和 LAB2 均100次测试无一FAIL。

LAB3 100次全部测试，出现了4次Fail，其中3次均为少append了仅一个数据，1次数据线性化失败。

因为这段时间debug过于劳累，这种少数情况出现的bug需要大量的精力在几十万条日志中排查，等有时间再解决它。

****

**测试结果均置于其文件夹下**

> src文件夹中含我的笔记(心得)



附课程链接: http://nil.csail.mit.edu/6.824/2021/labs/lab-kvraft.html



Todo: 线性化BUG出现概率极低也几乎无法排查(可能是其他的bug间接导致的，如果不是只可能是设计出现了问题)，先解决少append数据bug



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

