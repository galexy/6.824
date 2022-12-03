package kvraft

import (
	"6.824/labrpc"
	"6.824/logger"
	"fmt"
	"sync/atomic"
)
import "crypto/rand"
import "math/big"

type Clerk struct {
	servers    []*labrpc.ClientEnd
	lastLeader int64
}

func newSequence() SeqId {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return SeqId(x)
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// You'll have to add code here.
	return ck
}

func (kv *Clerk) Debug(format string, a ...interface{}) {
	prefix := fmt.Sprintf("[CL][%v] ", "KVCLNT")
	logger.DPrintf(prefix+format, a...)
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
	ck.Debug("Get(k=%v)", key)
	lastLeaderIndex := atomic.LoadInt64(&ck.lastLeader)
	lastLeader := ck.servers[lastLeaderIndex]

	isLeader, res := ck.tryGet(lastLeaderIndex, lastLeader, key)
	if isLeader {
		return res
	}

	for {
		for i, server := range ck.servers {
			isLeader, res := ck.tryGet(int64(i), server, key)
			if isLeader {
				atomic.StoreInt64(&ck.lastLeader, int64(i))
				return res
			}
		}
	}
}

func (ck *Clerk) tryGet(serverId int64, server *labrpc.ClientEnd, key string) (isLeader bool, val string) {
	args := GetArgs{SeqId: newSequence(), Key: key}
	reply := GetReply{}
	if ok := server.Call("KVServer.Get", &args, &reply); !ok {
		ck.Debug("S%d -> KVServer.Get(%v) failed", serverId, args.Key)
		return
	}

	if reply.Err == "" {
		ck.Debug("S%d -> KVServer.Get(%v) -> %v", serverId, args.Key, reply)
		return true, reply.Value
	}

	return
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
	ck.Debug("PutAppend(k=%v, v=%v)", key, value)

	lastLeaderIndex := atomic.LoadInt64(&ck.lastLeader)
	lastLeader := ck.servers[lastLeaderIndex]

	isLeader := ck.tryPutAppend(lastLeaderIndex, lastLeader, key, value, op)
	if isLeader {
		return
	}

	for {
		for i, server := range ck.servers {
			isLeader := ck.tryPutAppend(int64(i), server, key, value, op)
			if isLeader {
				atomic.StoreInt64(&ck.lastLeader, int64(i))
				return
			}
		}
	}
}

func (ck *Clerk) tryPutAppend(serverId int64, server *labrpc.ClientEnd, key string, value string, op string) bool {
	args := PutAppendArgs{SeqId: newSequence(), Key: key, Value: value, Op: op}
	reply := PutAppendReply{}

	if ok := server.Call("KVServer.PutAppend", &args, &reply); !ok {
		ck.Debug("S%d -> KVServer.PutAppend(%v) failed", serverId, args)
		return false
	}

	if reply.Err == "" {
		ck.Debug("S%d -> KVServer.PutAppend(%v) -> %v", serverId, args, reply)
		return true
	}

	return false
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
