package shardkv

import "log"

// 主要逻辑日志输出
const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	log.SetFlags(log.Lmicroseconds)
	if Debug {
		log.Printf(format, a...)
	}
	return
}

func DPrintFatal(format string, a ...interface{}) (n int, err error) {
	log.SetFlags(log.Lmicroseconds)
	log.Panicf(format, a...)
	return
}

// 更详细的debug日志信息
const info = false

func INFO(format string, a ...interface{}) (n int, err error) {
	log.SetFlags(log.Lmicroseconds)
	if info {
		log.Printf(format, a...)
	}
	return
}

// 注意无锁，外层加锁
func (kv *ShardKV) DprintfKVDB() {
	DPrintf("[=======KVDBINFO======]")
	DPrintf("[Who]Gid %d, server %d", kv.gid, kv.me)
	DPrintf("[isMigRating]%v", kv.migratingShard)
	DPrintf("[kvDB]%v", kv.kvDB)
	DPrintf("[Shards]%v", kv.config.Shards)
}
