package raft

import (
	"fmt"
	"log"
	"os"
	"strconv"
	"time"
)

// Debugging
const debug = false

// Retrieve the verbosity level from an environment variable
func getVerbosity() int {
	v := os.Getenv("VERBOSE")
	level := 0
	if v != "" {
		var err error
		level, err = strconv.Atoi(v)
		if err != nil {
			log.Fatalf("Invalid verbosity %v", v)
		}
	}
	return level
}

type logTopic string

const (
	dTerm  logTopic = "TERM" // Term 发生变化
	dState logTopic = "STAT" // 状态发生变化
	dTimer logTopic = "TIMR" // 定时器触发
	dVote  logTopic = "VOTE" // 投票消息
	dAlive logTopic = "LIVE" // Leader 发送心跳 keep alive
)

var debugStart time.Time
var debugVerbosity int

func init() {
	debugVerbosity = getVerbosity()
	debugStart = time.Now()

	log.SetFlags(log.Flags() &^ (log.Ldate | log.Ltime))
}

func Debug(topic logTopic, format string, a ...interface{}) {
	if debug {
		time := time.Since(debugStart).Microseconds()
		time /= 100
		prefix := fmt.Sprintf("%06d %v ", time, string(topic))
		format = prefix + format
		log.Printf(format, a...)
	}
}
