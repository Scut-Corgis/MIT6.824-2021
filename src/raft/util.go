package raft

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

func Max(a int, b int) int {
	if a > b {
		return a
	} else {
		return b
	}
}
func Min(a int, b int) int {
	if a < b {
		return a
	} else {
		return b
	}
}
