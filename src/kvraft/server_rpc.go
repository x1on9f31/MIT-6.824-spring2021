package kvraft

import logger "6.824/raft-logs"

//rpc handler
func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	request_arg := Command{
		OptType:  TYPE_GET,
		Opt:      args.Key,
		ClientID: args.ClientID,
		Seq:      args.Seq,
	}

	result := kv.doRequest(&request_arg).(*GetReply)
	reply.Err = result.Err
	reply.Value = result.Value
	kv.logger.L(logger.ServerReq, "[%3d--%d] get return result%#v\n",
		args.ClientID%1000, args.Seq, reply)

}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	request_arg := Command{
		Opt: KeyValue{
			Key:   args.Key,
			Value: args.Value,
		},
		ClientID: args.ClientID,
		Seq:      args.Seq,
	}
	switch args.Op {
	case "Put":
		request_arg.OptType = TYPE_PUT
	case "Append":
		request_arg.OptType = TYPE_APPEND
	default:
		kv.logger.L(logger.ServerReq, "putAppend err type %d from [%3d--%d]\n",
			args.Op, args.ClientID%1000, args.Seq)
	}

	reply_arg := kv.doRequest(&request_arg).(*PutAppendReply) //wait

	reply.Err = reply_arg.Err

	kv.logger.L(logger.ServerReq, "[%3d--%d] putAppend return result%#v\n",
		args.ClientID%1000, args.Seq, reply)
}
