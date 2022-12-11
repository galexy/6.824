package kvraft

import (
	"6.824/labgob"
	"6.824/labrpc"
	"6.824/logger"
	"6.824/raft"
	"fmt"
	"sync"
	"sync/atomic"
)

type SeqId uint64

type OpCode int

const (
	OpPut OpCode = iota
	OpAppend
	OpGet
	OpKill
)

type Op struct {
	ClientId int64
	Id       SeqId
	OpCode   OpCode
	Key      string
	Value    string
	ServerId raft.ServerId
}

type Resp struct {
	Id    SeqId
	err   Err
	value string
}

func (r Resp) String() string {
	return fmt.Sprintf("seq=%d, v=%s, e=%s", r.Id, r.value, r.err)
}

type Command struct {
	Op
	respCh chan Resp
}

func (c Command) String() string {
	code := ""
	switch c.OpCode {
	case OpPut:
		code = "put"
	case OpAppend:
		code = "append"
	case OpGet:
		code = "get"
	}
	return fmt.Sprintf("ID=%d, C=%v, K=%v, V=%v", c.Id, code, c.Key, c.Value)
}

type KVServer struct {
	mu           sync.Mutex
	me           raft.ServerId
	rf           *raft.Raft
	applyCh      chan raft.ApplyMsg
	dead         int32 // set by Kill()
	maxraftstate int   // snapshot if log grows this big

	smChannel      chan Command // Channel to queue commands
	respChannels   map[SeqId]chan Resp
	kvStore        map[string]string
	lastClientResp map[int64]Resp
}

func (kv *KVServer) Debug(format string, a ...interface{}) {
	prefix := fmt.Sprintf("[S%d][%v] S%d ", kv.me, "KVSERV", kv.me)
	logger.DPrintf(prefix+format, a...)
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	op := Op{ClientId: args.ClientId, Id: args.SeqId, OpCode: OpGet, Key: args.Key}
	respChn := make(chan Resp)
	cmd := Command{Op: op, respCh: respChn}

	kv.Debug("SM <- Get(%v)", args)
	kv.smChannel <- cmd
	resp := <-respChn
	kv.Debug("SM -> Get(%v) -> %v", args, resp)

	reply.Err = resp.err
	reply.Value = resp.value
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	opCode := OpPut
	if args.Op == "Append" {
		opCode = OpAppend
	}

	op := Op{ClientId: args.ClientId, Id: SeqId(args.SeqId), OpCode: opCode, Key: args.Key, Value: args.Value}
	respChn := make(chan Resp)
	cmd := Command{Op: op, respCh: respChn}

	kv.Debug("SM <- %v(%v)", args.Op, args)
	kv.smChannel <- cmd
	resp := <-respChn
	kv.Debug("SM -> %v(%v) -> %v", args.Op, args, resp)
	reply.Err = resp.err
}

//
// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
//
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = raft.ServerId(me)
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.smChannel = make(chan Command)
	kv.respChannels = make(map[SeqId]chan Resp)
	kv.kvStore = make(map[string]string)
	kv.lastClientResp = make(map[int64]Resp)

	// You may need initialization code here.
	go kv.process()

	return kv
}

func (kv *KVServer) process() {
	defer func() {
		panic("FAIL")
	}()

	for {
		kv.Debug("Waiting for command")
		select {
		case command := <-kv.smChannel:
			kv.Debug("command(%v) <- SM channel", command)

			resp, ok := kv.lastClientResp[command.ClientId]
			if ok && resp.Id == command.Id {
				kv.Debug("Repeated of previous command. Repeating response.")
				command.respCh <- resp
				continue
			}

			op := command.Op
			op.ServerId = kv.me
			_, _, isLeader := kv.rf.Start(op)

			// If the current server isn't leader, respond immediately
			if !isLeader {
				command.respCh <- Resp{Id: command.Id, err: "Not Leader"}
			} else {
				kv.respChannels[command.Id] = command.respCh
			}

		case message := <-kv.applyCh:
			kv.Debug("Received message from Raft: %v", message)

			var op Op
			var ok bool
			if op, ok = message.Command.(Op); !ok {
				panic("Could not cast command back")
			}

			// Get the response channel if the current server made the request while leader
			// It's possible that it has lost leadership in between however
			respCh, hasChannel := kv.respChannels[op.Id]
			stillLeader := op.ServerId == kv.me

			if hasChannel && !stillLeader {
				respCh <- Resp{Id: op.Id, err: "Not Leader"}
				continue
			}

			if prevResp, ok := kv.lastClientResp[op.ClientId]; ok && prevResp.Id == op.Id {
				kv.Debug("Repeated of previous command. Repeating response and not applying command.")
				respCh <- prevResp
				continue
			}

			var resp Resp
			switch op.OpCode {
			case OpPut:
				kv.kvStore[op.Key] = op.Value
				resp = Resp{Id: op.Id}
			case OpAppend:
				kv.kvStore[op.Key] += op.Value
				resp = Resp{Id: op.Id}
			case OpGet:
				val := kv.kvStore[op.Key]
				resp = Resp{Id: op.Id, value: val}
			}

			if hasChannel && stillLeader {
				respCh <- resp
			}
			kv.lastClientResp[op.ClientId] = resp

			if hasChannel {
				delete(kv.respChannels, op.Id)
			}
		}
	}
}
