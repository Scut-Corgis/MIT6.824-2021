count=100
    for i in $(seq 76 ${count})
    do
        echo "第 ${i} 轮测试" >> out4.log 
        echo "-------------------------------------" >> out4.log 
        go test  >> out4.log 
    done

