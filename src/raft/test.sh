count=100
    for i in $(seq $count); do
        go test -run 2B -race >>out.log
        done
