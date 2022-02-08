#!/bin/bash
count=100
file="out.log"
    for i in $(seq 1 ${count})
    do
        echo "第 ${i} 轮测试" >> $file
        echo "-------------------------------------" >> $file
        go test -run TestSnapshotUnreliableRecoverConcurrentPartition3B >> $file
        grep -n "FAIL" $file
        if [ $? -eq 0 ]; then 
        echo "出现错误，程序已经退出,错误出现在第 ${i} 轮"
        exit
        else 
        rm $file
        echo "第 ${i} 轮测试成功"
        fi
    done
