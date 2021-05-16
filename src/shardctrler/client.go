package shardctrler

//
// Shardctrler clerk.
//

import (
	"crypto/rand"
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
	// Your data here.
	id  int64
	seq int
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
	// Your code here.
	return ck
}

func (ck *Clerk) Query(num int) Config {
	args := &QueryArgs{
		ClientID: ck.id,
		Seq:      ck.seq,
		Num:      num,
	}
	for {
		// Your code here.
		// try each known server.
		for _, srv := range ck.servers {
			var reply QueryReply
			ok := srv.Call("ShardCtrler.Query", args, &reply)
			if ok && !reply.WrongLeader {
				ck.seq++
				return reply.Config
			}

		}
		time.Sleep(time.Millisecond * 100)
	}
}

func (ck *Clerk) Join(servers map[int][]string) {

	// Your code here.
	args := &JoinArgs{
		ClientID: ck.id,
		Seq:      ck.seq,
		Servers:  servers,
	}

	for {
		// try each known server.
		for _, srv := range ck.servers {

			var reply JoinReply
			ok := srv.Call("ShardCtrler.Join", args, &reply)
			if !ok {
				reply.WrongLeader = true
			}

			if ok && !reply.WrongLeader {
				ck.seq++
				return
			}

		}
		time.Sleep(time.Millisecond * 100)
	}
}

func (ck *Clerk) Leave(gids []int) {
	args := &LeaveArgs{
		ClientID: ck.id,
		Seq:      ck.seq,
		GIDs:     gids,
	}
	for {

		// Your code here.

		// try each known server.
		for _, srv := range ck.servers {

			var reply LeaveReply
			ok := srv.Call("ShardCtrler.Leave", args, &reply)

			if ok && !reply.WrongLeader {
				ck.seq++
				return
			}

		}
		time.Sleep(time.Millisecond * 100)
	}
}

func (ck *Clerk) Move(shard int, gid int) {

	// Your code here.
	args := &MoveArgs{
		Shard:    shard,
		GID:      gid,
		ClientID: ck.id,
		Seq:      ck.seq,
	}
	for {
		// try each known server.
		for _, srv := range ck.servers {

			var reply MoveReply
			ok := srv.Call("ShardCtrler.Move", args, &reply)
			if !ok {
				reply.WrongLeader = true
			}

			if ok && !reply.WrongLeader {
				ck.seq++
				return
			}

		}
		time.Sleep(time.Millisecond * 100)
	}
}
