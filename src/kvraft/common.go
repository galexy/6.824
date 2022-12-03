package kvraft

import "fmt"

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongLeader = "ErrWrongLeader"
)

type Err string

// Put or Append
type PutAppendArgs struct {
	SeqId SeqId
	Key   string
	Value string
	Op    string // "Put" or "Append"
}

func (a PutAppendArgs) String() string {
	return fmt.Sprintf("id= %d, op=%s, k=%s, v=%s", a.SeqId, a.Op, a.Key, a.Value)
}

type PutAppendReply struct {
	Err Err
}

func (r PutAppendReply) String() string {
	return fmt.Sprintf("e=%s", r.Err)
}

type GetArgs struct {
	SeqId SeqId
	Key   string
}

func (a GetArgs) String() string {
	return fmt.Sprintf("id= %d, k=%s", a.SeqId, a.Key)
}

type GetReply struct {
	Err   Err
	Value string
}
