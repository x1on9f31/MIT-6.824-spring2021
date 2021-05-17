package shardctrler

import "fmt"

func (sc *ShardCtrler) Join(args *JoinArgs, reply *JoinReply) {
	sc.mu.Lock()
	fmt.Printf("join %v\n", args)
	command := Command{
		ClientID: args.ClientID,
		Seq:      args.Seq,
		OptType:  TYPE_JOIN,
		Opt:      *args,
	}
	res := sc.doRequest(&command) //drop lock
	reply.WrongLeader = res.(*JoinReply).WrongLeader
}

func (sc *ShardCtrler) Move(args *MoveArgs, reply *MoveReply) {
	sc.mu.Lock()
	fmt.Printf("move %v\n", args)
	command := Command{
		ClientID: args.ClientID,
		Seq:      args.Seq,
		OptType:  TYPE_MOVE,
		Opt: GIDandShard{
			GID:   args.GID,
			Shard: args.Shard,
		},
	}
	res := sc.doRequest(&command) //drop lock
	reply.WrongLeader = res.(*MoveReply).WrongLeader

}

func (sc *ShardCtrler) Leave(args *LeaveArgs, reply *LeaveReply) {
	sc.mu.Lock()
	fmt.Printf("leave %v\n", args)
	command := Command{
		ClientID: args.ClientID,
		Seq:      args.Seq,
		OptType:  TYPE_LEAVE,
		Opt:      args.GIDs,
	}
	res := sc.doRequest(&command) //drop lock
	reply.WrongLeader = res.(*LeaveReply).WrongLeader
}

func (sc *ShardCtrler) Query(args *QueryArgs, reply *QueryReply) {
	sc.mu.Lock()
	command := Command{
		ClientID: args.ClientID,
		Seq:      args.Seq,
		OptType:  TYPE_QUERY,
		Opt:      args.Num,
	}
	res := sc.doRequest(&command) //drop lock
	ress := res.(*QueryReply)
	reply.WrongLeader = ress.WrongLeader
	reply.Config = ress.Config
	//fmt.Printf("server sent reply %#v\n", reply)
}
