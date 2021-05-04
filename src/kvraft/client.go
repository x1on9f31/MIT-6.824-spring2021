package kvraft

import (
	"crypto/rand"
	"fmt"
	"math/big"
	"sync"
	"time"

	"6.824/labrpc"
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
	id        int64
	serverCnt int

	cmd_seq    int
	lastLeader int
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
	ck.serverCnt = len(servers)
	ck.id = getUnusedClientID()
	ck.cmd_seq = 0
	// You'll have to add code here.
	time.Sleep(1 * time.Second)
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

	// You will have to modify this function.

	for {
		args := GetArgs{
			Key:      key,
			ClientID: ck.id,
		}
		timer := time.NewTimer(time.Millisecond * 100)
		done := make(chan *GetReply)
		args.Cmd_Seq = ck.cmd_seq

		peer := ck.lastLeader
		go func(d chan *GetReply) {
			reply := GetReply{
				Err:   "",
				Value: "",
			}
			ok := ck.servers[peer].Call("KVServer.Get", &args, &reply)
			if !ok {
				reply.Err = "!ok rpc"
			}
			d <- &reply
		}(done)

		select {
		case <-timer.C:
		case reply := <-done:
			if reply.Err == "" {
				fmt.Printf("[%3d--%d] clerk get okkkkk : %v\n", ck.id%1000, args.Cmd_Seq, reply.Value)

				ck.cmd_seq++

				return reply.Value
			}
		}

		ck.lastLeader = (ck.lastLeader + 1) % ck.serverCnt

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

	for {
		args := PutAppendArgs{
			Key:      key,
			Value:    value,
			Op:       op,
			ClientID: ck.id,
		}
		timer := time.NewTimer(time.Millisecond * 100)
		done := make(chan *PutAppendReply)

		args.Cmd_Seq = ck.cmd_seq

		peer := ck.lastLeader
		go func(d chan *PutAppendReply) {
			reply := PutAppendReply{
				Err: "",
			}
			ok := ck.servers[peer].Call("KVServer.PutAppend", &args, &reply)
			if !ok {
				reply.Err = "!ok rpc"
			}
			done <- &reply
		}(done)

		select {
		case <-timer.C:
		case reply := <-done:
			if reply.Err == "" {
				fmt.Printf("[%3d--%d] clerk putAppend okkkkk\n", ck.id%1000, args.Cmd_Seq)

				ck.cmd_seq++

				return
			}
			// else {
			// 	fmt.Printf("[%3d--%d] clerk putAppend not ok err: %v\n", ck.id%1000, ck.cmd_seq, reply.Err)
			// }
		}

		ck.lastLeader = (ck.lastLeader + 1) % ck.serverCnt

	}

}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
