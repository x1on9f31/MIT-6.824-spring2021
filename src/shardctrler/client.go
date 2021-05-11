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
	id         int64
	serverCnt  int
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
	// Your code here.
	return ck
}

func (ck *Clerk) Query(num int) Config {
	args := &QueryArgs{
		ClientID: ck.id,
		Cmd_Seq:  ck.cmd_seq,
		Num:      num,
	}
	for {
		// Your code here.
		// try each known server.
		for _, srv := range ck.servers {
			var reply QueryReply
			ok := srv.Call("ShardCtrler.Query", args, &reply)
			if ok && reply.WrongLeader == false {
				//fmt.Printf("client got %#v\n", reply)
				ck.cmd_seq++
				return reply.Config
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Join(servers map[int][]string) {
	args := &JoinArgs{
		ClientID: ck.id,
		Cmd_Seq:  ck.cmd_seq,
		Servers:  servers,
	}
	// Your code here.

	for {

		// try each known server.

		for _, srv := range ck.servers {
			var reply JoinReply
			ok := srv.Call("ShardCtrler.Join", args, &reply)
			if ok && reply.WrongLeader == false {
				//fmt.Println(reply)
				ck.cmd_seq++
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Leave(gids []int) {

	args := &LeaveArgs{
		ClientID: ck.id,
		Cmd_Seq:  ck.cmd_seq,
		GIDs:     gids,
	}
	for {

		// Your code here.

		// try each known server.
		for _, srv := range ck.servers {
			var reply LeaveReply
			ok := srv.Call("ShardCtrler.Leave", args, &reply)
			if ok && reply.WrongLeader == false {
				ck.cmd_seq++
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Move(shard int, gid int) {
	args := &MoveArgs{
		Shard:    shard,
		GID:      gid,
		ClientID: ck.id,
		Cmd_Seq:  ck.cmd_seq,
	}
	// Your code here.

	for {
		// try each known server.
		for _, srv := range ck.servers {
			var reply MoveReply
			ok := srv.Call("ShardCtrler.Move", args, &reply)
			if ok && reply.WrongLeader == false {
				ck.cmd_seq++
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}
