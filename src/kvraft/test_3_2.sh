count=50
    for i in $(seq 26 ${count})
    do
        echo "第 ${i} 轮测试" >> out2.log 
        echo "-------------------------------------" >> out2.log 
        go test  >> out2.log 
    done

