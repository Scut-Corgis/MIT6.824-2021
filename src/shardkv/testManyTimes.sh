count=100
file="100次测试20220216.log"
    for i in $(seq 1 ${count})
    do
        echo "第 ${i} 轮测试" >> $file
        echo "-------------------------------------" >> $file
        go test  >> $file 
    done

