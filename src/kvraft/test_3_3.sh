count=75
    for i in $(seq 51 ${count})
    do
        echo "第 ${i} 轮测试" >> out3.log 
        echo "-------------------------------------" >> out3.log 
        go test  >> out3.log 
    done

