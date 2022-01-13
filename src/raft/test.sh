count=10
    for i in $(seq $count); do
        go test -run 2B >> out.log -race
        done

