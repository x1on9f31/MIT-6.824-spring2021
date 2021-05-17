package raftlogs

import (
	"fmt"
	"log"
	"os"
	"strconv"
	"time"
)

type LogTopic int

const (
	RAFT_IGNORE    = true
	COMMIT_IGNORE  = false //apply,commit
	TIMER_IGNORE   = true  //timer
	LEADER_IGNORE  = true  //leader
	APPEND_IGNORE  = true  //append
	ROLE_IGNORE    = false //term vote
	PERSIST_IGNORE = false //persist log

	//raft
	Client LogTopic = iota
	Commit
	Drop
	Error
	Info
	Leader
	Log1
	Persist
	Snap
	Term
	Test
	Timer
	Trace
	Vote
	Warn
	Log2
	RaftShutdown
	Role
	Apply
	Append
	SnapSize

	//kv server
	ServerReq
	ServerSnap
	ServerApply
	ServerSnapSize
	ServerShutdown
	ServerStart
	ServerConfig
	ServerMove

	//kv clerk
	Clerk

	//test config
	Cfg

	//ctrler
	CtrlerStart
	Query
	Join
	Leave
	Move
	Balance
	CtrlerApply
	CtrlerSnap
	CtrlerReq
	CtrlerSnapSize

	//shardkv
	SKVReq = 100
	SKVStart
	SKVApply
	SKVConfig
	SKVMove
	SKVSnap
	SKVSnapSize
	SKVShutDown
	Debug_Level = 2
)

var debugStart time.Time
var debugVerbosity int
var Print_Map map[LogTopic]bool

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
func init() {
	debugVerbosity = getVerbosity()
	debugStart = time.Now()

	log.SetFlags(log.Flags() &^ (log.Ldate | log.Ltime))
	Print_Map = make(map[LogTopic]bool)
	print_list := []LogTopic{
		Clerk,
		ServerApply,
		ServerReq,
		// Leader,
		SnapSize,
		ServerSnapSize,
		ServerStart,
		//ServerSnapSize,
		ServerShutdown,
		ServerConfig,
		ServerMove,
		ServerSnap,
		// ServerShutdown,
		// Cfg,
		// ServerApply,
		//CtrlerStart,
		CtrlerApply,
		//Role,
		//Term,
		//Timer,
		//Leader,
		//Log1,
		//Client,
		// Query,
		// Join,
		// Leave,
		// Move,
		// Balance,
	}

	for _, v := range print_list {
		Print_Map[v] = true
	}
}

type TopicLogger struct {
	Me int
}

func (tp *TopicLogger) L(topic LogTopic, format string, a ...interface{}) {
	if Print_Map[topic] {
		time := time.Since(debugStart).Milliseconds()
		time_seconds := time / 1000
		time = time % 1000
		prefix := fmt.Sprintf("%03d'%03d %d S%d ", time_seconds, time, topic, tp.Me)
		format = prefix + format
		log.Printf(format, a...)
	}
}
