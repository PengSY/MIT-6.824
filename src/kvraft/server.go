package raftkv

import (
	"encoding/gob"
	"labrpc"
	"log"
	"raft"
	"sync"
	"fmt"
	"bytes"
)

const Debug = 0
const KILL=0

type KillMsg int

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
	Id int
	LeaderId int
	Key string
	Value string
	Type string
	CkId int64
	replyCh chan interface{}
}

type RaftKV struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	persister *raft.Persister
	Kvdb KVDatabase
	ClerkCommitOps map[int64]int

	killmsgCh chan KillMsg
	isDead bool
}

func (kv *RaftKV) PrintLog(format string, a ...interface{}){
	if Debug ==0 || kv.isDead{
		return
	}
	fmt.Println(fmt.Sprintf("s%d:",kv.me)+fmt.Sprintf(format,a...))
}

func (kv *RaftKV) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	var op Op
	var replyCh chan interface{}

	replyCh=make(chan interface{})
	op.Id=args.Id
	op.LeaderId=kv.me
	op.Key=args.Key
	op.Type="Get"
	op.CkId=args.CkId
	op.replyCh=replyCh

	if _,_,isLeader:=kv.rf.Start(op);!isLeader{
		reply.WrongLeader=true
		return
	}
	kv.PrintLog("recieve Get request(id=%d)",op.Id)

	kv.PrintLog("waiting for agreement(id=%d)",op.Id)
	replyInf:=<-replyCh
	*reply=replyInf.(GetReply)
}

func (kv *RaftKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	var op Op
	var replyCh chan interface{}

	if _,isLeader:=kv.rf.GetState();!isLeader{
		reply.WrongLeader=true
		return
	}

	kv.PrintLog("recieve PutAppend request(id=%d)",args.Id)
	kv.mu.Lock()
	if opid,ok:=kv.ClerkCommitOps[args.CkId];ok && opid>=args.Id{
		reply.WrongLeader=false
		reply.Err=OK
		kv.mu.Unlock()
		return
	}
	kv.mu.Unlock()

	replyCh=make(chan interface{})
	op.Id=args.Id
	op.LeaderId=kv.me
	op.Key=args.Key
	op.Value=args.Value
	op.Type=args.Op
	op.CkId=args.CkId
	op.replyCh=replyCh

	if _,_,isLeader:=kv.rf.Start(op);!isLeader{
		reply.WrongLeader=true
		return
	}
	kv.PrintLog("waiting for agreement(id=%d)",op.Id)
	replyInf:=<-replyCh
	*reply=replyInf.(PutAppendReply)
}

//
// the tester calls Kill() when a RaftKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (kv *RaftKV) Kill() {
	// Your code here, if desired.
	//kv.PrintLog("I am going to kill myself")
	kv.rf.Kill()
	kv.killmsgCh<-KILL
	//kv.PrintLog("kill return")
	kv.isDead=true
}

func (kv *RaftKV) ApplyGet(op Op){
	if op.LeaderId!=kv.me || op.replyCh==nil{
		return
	}

	var ok bool
	var reply GetReply
	reply.WrongLeader=false
	reply.Value,ok=kv.Kvdb.Get(op.Key)
	op.Value=reply.Value
	if ok{
		reply.Err=OK
	}else{
		reply.Err=ErrNoKey
	}
	kv.PrintLog("complete aggrement, return GetReply,client=%d,id=%d",op.CkId,op.Id)
	op.replyCh<-reply
}

func (kv *RaftKV) ApplyPutAppend(op Op){
	isDup:=false
	kv.mu.Lock()
	if opid,ok:=kv.ClerkCommitOps[op.CkId];ok{
		if opid>=op.Id{
			isDup=true
		}else{
			kv.ClerkCommitOps[op.CkId]=op.Id
		}
	}else{
		kv.ClerkCommitOps[op.CkId]=op.Id
	}
	kv.mu.Unlock()

	if !isDup{
		if op.Type == "Put" {
			kv.Kvdb.Put(op.Key, op.Value)
		} else {
			kv.Kvdb.Append(op.Key, op.Value)
		}
	}
	if op.LeaderId!=kv.me || op.replyCh==nil{
		return
	}
	var reply PutAppendReply
	reply.WrongLeader=false
	reply.Err=OK
	kv.PrintLog("complete aggrement, return PutAppendReply,client=%d,id=%d",op.CkId,op.Id)
	op.replyCh<-reply
}

func (kv *RaftKV) ApplyRoutine(){
	for {
		select {
		case <-kv.killmsgCh:
			kv.PrintLog("I am dead")
			return
		case msg := <-kv.applyCh:
			if msg.UseSnapshot{
				kv.persister.SaveSnapshot(msg.Snapshot)
				reader:=bytes.NewBuffer(msg.Snapshot)
				decoder:=gob.NewDecoder(reader)
				decoder.Decode(kv)
			}else{
				op := msg.Command.(Op)
				kv.PrintLog("agreement reached(id=%d,index=%d)", op.Id,msg.Index)
				switch op.Type{
				case "Get":
					kv.ApplyGet(op)
				case "Put":
					fallthrough
				case "Append":
					kv.ApplyPutAppend(op)
				}
				if kv.maxraftstate>0 && kv.persister.RaftStateSize()>=kv.maxraftstate{
					kv.mu.Lock()
					kv.PrintLog("begin snapshot")
					writer := new(bytes.Buffer)
					encoder := gob.NewEncoder(writer)
					encoder.Encode(kv)
					snapshot := writer.Bytes()
					kv.persister.SaveSnapshot(snapshot)
					kv.rf.GarbageCollect(msg.Index)
					kv.PrintLog("finish snapshot")
					kv.mu.Unlock()
				}
			}

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
	kv.persister=persister

	// You may need initialization code here.
	kv.killmsgCh=make(chan KillMsg)
	kv.isDead=false
	kv.ClerkCommitOps=make(map[int64]int)
	kv.Kvdb.Make()

	reader:=bytes.NewBuffer(kv.persister.ReadSnapshot())
	decoder:=gob.NewDecoder(reader)
	decoder.Decode(kv)

	go kv.ApplyRoutine()

	return kv
}
