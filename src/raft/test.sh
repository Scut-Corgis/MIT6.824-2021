count=100
    for i in $(seq 1 ${count})
    do
        echo "第 ${i} 轮测试" >> out.log 
        echo "-------------------------------------" >> out.log 
        go test -run 2D>> out.log -race
    done

