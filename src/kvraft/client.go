package kvraft

import (
	"crypto/rand"
	"math/big"
	"sync"

	"6.824/labrpc"
	logger "6.824/raft-logs"
)

var used_ID map[int64]bool
var map_lock sync.Mutex

func init() {
	used_ID = make(map[int64]bool)
	used_ID[-1] = true
}

func getUnusedClientID() int64 {
	map_lock.Lock()
	defer map_lock.Unlock()
	for {
		id := nrand()
		if !used_ID[id] {
			used_ID[id] = true
			return id
		}
	}
}

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	id     int64
	logger logger.TopicLogger
	seq    int
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	ck.id = getUnusedClientID()
	ck.seq = 0
	ck.logger = logger.TopicLogger{Me: int(ck.id) % 1000}
	// You'll have to add code here.
	// time.Sleep(1 * time.Second)
	return ck
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) Get(key string) string {
	args := GetArgs{
		Key:      key,
		ClientID: ck.id,
		Seq:      ck.seq,
	}
	// You will have to modify this function.

	for {
		for _, srv := range ck.servers {

			reply := GetReply{}
			ok := srv.Call("KVServer.Get", &args, &reply)

			if ok && reply.Err == OK {
				ck.logger.L(logger.Clerk, "[%d] clerk get okkkkk : %v\n", args.Seq, reply.Value)
				ck.seq++
				return reply.Value
			}

		}
	}

}

//
// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
	args := PutAppendArgs{
		Key:      key,
		Value:    value,
		Op:       op,
		ClientID: ck.id,
		Seq:      ck.seq,
	}
	for {
		for _, srv := range ck.servers {

			reply := PutAppendReply{}
			ok := srv.Call("KVServer.PutAppend", &args, &reply)

			if ok && reply.Err == OK {
				ck.logger.L(logger.Clerk, "[%d] clerk putAppend okkkkk\n", args.Seq)
				ck.seq++
				return
			}

		}
	}

}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
