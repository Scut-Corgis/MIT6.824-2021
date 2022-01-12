package raft

import "log"

// Debugging
const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	log.SetFlags(log.Lshortfile | log.Lmicroseconds)
	if Debug {
		log.Printf(format, a...)
	}
	return
}
