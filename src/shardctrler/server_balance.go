package shardctrler

func (sc *ShardCtrler) MakeMoveConfig(shard int, gid int) *Config {
	lastConfig := sc.configs[len(sc.configs)-1]
	tempConfig := Config{Num: len(sc.configs),
		Shards: [10]int{},
		Groups: map[int][]string{}}
	for shards, gids := range lastConfig.Shards {
		tempConfig.Shards[shards] = gids
	}
	tempConfig.Shards[shard] = gid

	for gidss, servers := range lastConfig.Groups {
		tempConfig.Groups[gidss] = servers
	}

	return &tempConfig
}

func (sc *ShardCtrler) MakeJoinConfig(servers map[int][]string) *Config {

	lastConfig := sc.configs[len(sc.configs)-1]
	tempGroups := make(map[int][]string)

	for gid, serverList := range lastConfig.Groups {
		tempGroups[gid] = serverList
	}
	for gids, serverLists := range servers {
		tempGroups[gids] = serverLists
	}

	// 统计每个group对应多少个shard分片  Gid --> 分片数目
	GidToShardNumMap := make(map[int]int)
	for gid := range tempGroups {
		GidToShardNumMap[gid] = 0
	}
	for _, gid := range lastConfig.Shards {
		if gid != 0 {
			GidToShardNumMap[gid]++
		}
	}

	if len(GidToShardNumMap) == 0 {
		return &Config{
			Num:    len(sc.configs),
			Shards: [10]int{},
			Groups: tempGroups,
		}
	}
	return &Config{
		Num:    len(sc.configs),
		Shards: sc.reBalanceShards(GidToShardNumMap, lastConfig.Shards),
		Groups: tempGroups,
	}
}

func (sc *ShardCtrler) MakeLeaveConfig(gids []int) *Config {

	lastConfig := sc.configs[len(sc.configs)-1]
	tempGroups := make(map[int][]string)

	ifLeaveSet := make(map[int]bool)
	for _, gid := range gids {
		ifLeaveSet[gid] = true
	}
	// 设置新的Group映射，即删除了离开的集群
	for gid, serverList := range lastConfig.Groups {
		tempGroups[gid] = serverList
	}
	for _, gidLeave := range gids {
		delete(tempGroups, gidLeave)
	}
	// 将离开组的原有分片对应组号重置为0，然后统计每个组对应的分片数据，用于后面的负载均衡
	newShard := lastConfig.Shards
	GidToShardNumMap := make(map[int]int)
	for gid := range tempGroups {
		if !ifLeaveSet[gid] {
			GidToShardNumMap[gid] = 0
		}

	}
	for shard, gid := range lastConfig.Shards {
		if gid != 0 {
			if ifLeaveSet[gid] {
				newShard[shard] = 0
			} else {
				GidToShardNumMap[gid]++
			}
		}

	}
	if len(GidToShardNumMap) == 0 {
		return &Config{
			Num:    len(sc.configs),
			Shards: [10]int{},
			Groups: tempGroups,
		}
	}
	return &Config{
		Num:    len(sc.configs),
		Shards: sc.reBalanceShards(GidToShardNumMap, newShard),
		Groups: tempGroups,
	}
}

// 参数： GidToShardNumMap 负载均衡前每个组对应的分片数量
//       lastShards  负载均衡前分片对应的组的分布情况
func (sc *ShardCtrler) reBalanceShards(GidToShardNumMap map[int]int, lastShards [NShards]int) [NShards]int {
	length := len(GidToShardNumMap)
	average := NShards / length
	subNum := NShards % length
	realSortNum := realNumArray(GidToShardNumMap)
	// 因realSortNum为从小到大顺序，反向遍历即从拥有最多shard的组开始处理
	for i := length - 1; i >= 0; i-- {
		// resultNum为该组应该拥有的分片数量
		resultNum := average
		if !ifAvg(length, subNum, i) {
			resultNum = average + 1
		}
		// 如果实际拥有的分片数量大于应该拥有的，则将多出的shard对应的group号码先置为0
		if resultNum < GidToShardNumMap[realSortNum[i]] {
			fromGid := realSortNum[i]
			changeNum := GidToShardNumMap[fromGid] - resultNum
			// 循环置0，直到达到应该拥有的分片数量。循环结束后，最终会有清理出来的分配没有对应任何group
			for shard, gid := range lastShards {
				if changeNum <= 0 {
					break
				}
				if gid == fromGid {
					lastShards[shard] = 0
					changeNum -= 1
				}
			}
			GidToShardNumMap[fromGid] = resultNum
		}
	}

	// 前面过程的反向操作， 即将空出来的分片分给比预期数量少的组
	for i := 0; i < length; i++ {
		resultNum := average
		if !ifAvg(length, subNum, i) {
			resultNum = average + 1
		}
		if resultNum > GidToShardNumMap[realSortNum[i]] {
			toGid := realSortNum[i]
			changeNum := resultNum - GidToShardNumMap[toGid]
			for shard, gid := range lastShards {
				if changeNum <= 0 {
					break
				}
				if gid == 0 {
					lastShards[shard] = toGid
					changeNum -= 1
				}
			}
			GidToShardNumMap[toGid] = resultNum
		}

	}
	return lastShards
}

// 返回值：按拥有分片数目从小到大排序好的组id
func realNumArray(GidToShardNumMap map[int]int) []int {
	length := len(GidToShardNumMap)

	numArray := make([]int, 0, length)
	for gid, _ := range GidToShardNumMap {
		numArray = append(numArray, gid)
	}
	// 冒泡排序， 按每个组拥有的分片数目从小到大排序， 若两个组分片数目相同则按组号码从小到大排序
	for i := 0; i < length-1; i++ {
		for j := length - 1; j > i; j-- {
			if GidToShardNumMap[numArray[j]] < GidToShardNumMap[numArray[j-1]] || (GidToShardNumMap[numArray[j]] == GidToShardNumMap[numArray[j-1]] && numArray[j] < numArray[j-1]) {
				numArray[j], numArray[j-1] = numArray[j-1], numArray[j]
			}
		}
	}
	return numArray
}

func ifAvg(length int, subNum int, i int) bool {
	if i < length-subNum {
		return true
	} else {
		return false
	}
}
