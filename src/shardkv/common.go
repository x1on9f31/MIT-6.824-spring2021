package shardkv

//
// Sharded key/value server.
// Lots of replica groups, each running Raft.
// Shardctrler decides which group serves each shard.
// Shardctrler may change shard assignment from time to time.
//
// You will have to modify these definitions.
//

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongGroup  = "ErrWrongGroup"
	ErrWrongLeader = "ErrWrongLeader"
)

type Err string

// Put or Append
type PutAppendArgs struct {
	// You'll have to add definitions here.
	Key   string
	Value string
	Op    string // "Put" or "Append"
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	ClientID int64
	Seq      int
	Num      int
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
	ClientID int64
	Seq      int
	Num      int
}

type GetReply struct {
	Err   Err
	Value string
}

type SendShardsArgs struct {
	Shards []ShardIndexed
	Num    int
}
type SendShardsReply struct {
	Ok  bool
	Num int
}

type ShardState struct {
	KVmap   map[string]string
	NextSeq map[int64]int
}

type ShardIndexed struct {
	ShardIndex int
	State      ShardState
}
type MigrateCommand = SendShardsArgs
