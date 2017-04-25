package raftkv

import (
	"encoding/gob"
	"labrpc"
	"log"
	"raft"
	"sync"
	"fmt"
	"time"
)

const Debug = 1

const ResendTimeout=5

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Id int64
	LeaderId int
	Key string
	Value string
	Type string
	replyCh chan interface{}
}

type RaftKV struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	kvdb KVDatabase
	commitOpList []int64
	isDead bool
}

func (kv *RaftKV) PrintLog(format string, a ...interface{}){
	if Debug > 0 {
		fmt.Println(fmt.Sprintf("s%d:",kv.me)+fmt.Sprintf(format,a...))
	}
}

func (kv *RaftKV) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	var timer *time.Timer
	var op Op
	var replyCh chan interface{}

	replyCh=make(chan interface{})
	op.Id=args.Id
	op.LeaderId=kv.me
	op.Key=args.Key
	op.Type="Get"
	op.replyCh=replyCh

	kv.mu.Lock()
	if _,_,isLeader:=kv.rf.Start(op);!isLeader{
		reply.WrongLeader=true
		kv.mu.Unlock()
		return
	}
	timer=time.NewTimer(ResendTimeout*time.Second)
	kv.mu.Unlock()
	select{
	case <-timer.C:
		reply.WrongLeader=true
		kv.PrintLog("resend timeout,id=%d",op.Id)
	case replyInf:=<-replyCh:
		*reply=replyInf.(GetReply)
		kv.PrintLog("complete aggrement, return GetReply,id=%d",op.Id)
	}
}

func (kv *RaftKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	var timer *time.Timer
	var op Op
	var replyCh chan interface{}

	replyCh=make(chan interface{})
	op.Id=args.Id
	op.LeaderId=kv.me
	op.Key=args.Key
	op.Value=args.Value
	op.Type=args.Op
	op.replyCh=replyCh

	kv.mu.Lock()
	if _,_,isLeader:=kv.rf.Start(op);!isLeader{
		reply.WrongLeader=true
		kv.mu.Unlock()
		return
	}
	timer=time.NewTimer(ResendTimeout*time.Second)
	kv.mu.Unlock()
	select{
	case <-timer.C:
		reply.WrongLeader=true
		kv.PrintLog("resend timeout,id=%d",op.Id)
	case replyInf:=<-replyCh:
		*reply=replyInf.(PutAppendReply)
		kv.PrintLog("complete aggrement, return PutAppendReply,id=%d",op.Id)
	}
}

//
// the tester calls Kill() when a RaftKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (kv *RaftKV) Kill() {
	kv.rf.Kill()
	// Your code here, if desired.
	kv.mu.Lock()
	defer kv.mu.Unlock()
	kv.isDead=true
}

func (kv *RaftKV) ApplyGet(op Op){
	var ok bool
	var reply GetReply
	reply.WrongLeader=false
	reply.Value,ok=kv.kvdb.Get(op.Key)
	if ok{
		reply.Err=OK
	}else{
		reply.Err=ErrNoKey
	}
	op.replyCh<-reply
}

func (kv *RaftKV) ApplyPutAppend(op Op,isDup bool){
	if !isDup{
		if op.Type == "Put" {
			kv.kvdb.Put(op.Key, op.Value)
		} else {
			kv.kvdb.Append(op.Key, op.Value)
		}
		kv.commitOpList = append(kv.commitOpList, op.Id)
	}
	if op.LeaderId!=kv.me{
		return
	}
	var reply PutAppendReply
	reply.WrongLeader=false
	reply.Err=OK
	op.replyCh<-reply
}

func (kv *RaftKV) ApplyRoutine(){
	for{
		kv.mu.Lock()
		if kv.isDead{
			kv.rf.Kill()
			kv.mu.Unlock()
			return
		}
		kv.mu.Unlock()
		msg:=<-kv.applyCh
		op:=msg.Command.(Op)
		//duplicate detection
		isDup:=false
		for _,opId:=range kv.commitOpList{
			if opId==op.Id{
				isDup=true
			}
		}
		switch op.Type{
		case "Get":
			kv.ApplyGet(op)
		case "Put":
			fallthrough
		case "Append":
			kv.ApplyPutAppend(op,isDup)
		}
	}
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots with persister.SaveSnapshot(),
// and Raft should save its state (including log) with persister.SaveRaftState().
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *RaftKV {
	// call gob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	gob.Register(Op{})

	kv := new(RaftKV)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg,100)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.
	kv.isDead=false
	kv.kvdb.Make()
	go kv.ApplyRoutine()

	return kv
}
