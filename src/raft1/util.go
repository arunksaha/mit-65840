package raft

import "log"

// Debugging
const Debug = false

func DPrintf(format string, a ...interface{}) {
	if Debug {
		log.Printf(format, a...)
	}
}

const debugLogEnabled = false

func DLog(format string, a ...interface{}) {
	if debugLogEnabled {
		log.Printf(format, a...)
	}
}

const traceEnabled = false

func DTrace(format string, a ...interface{}) {
	if traceEnabled {
		log.Printf(format, a...)
	}
}
