package raftkv

import (
	"encoding/gob"
	"labrpc"
	"log"
	"raft"
	"sync"
	"fmt"
)

const Debug = 0

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
	uncommitOpMap map[int64]Op
	commitOpList []int64
	isDead bool
	term int
}

func (kv *RaftKV) PrintLog(format string, a ...interface{}){
	if Debug > 0 {
		fmt.Printf("server%d:",kv.me)
		fmt.Printf(format,a...)
		fmt.Println()
	}
}

func (kv *RaftKV) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	var op Op
	var isLeader bool
	var curTerm int
	var replyCh chan interface{}

	replyCh=make(chan interface{})
	op.Id=args.Id
	op.Key=args.Key
	op.LeaderId=kv.me
	op.Type="Get"
	op.replyCh=replyCh

	kv.PrintLog("recieve Get request from client, Key=%s, id=%d",args.Key,args.Id)

	kv.mu.Lock()
	if _,curTerm,isLeader=kv.rf.Start(op);!isLeader{
		reply.WrongLeader=true
		kv.mu.Unlock()
		return
	}
	if curTerm!=kv.term{
		for _,op:=range kv.uncommitOpMap{
			if op.Type=="Get"{
				var reply GetReply
				reply.WrongLeader=true
				op.replyCh<-reply
			}else{
				var reply PutAppendReply
				reply.WrongLeader=true
				op.replyCh<-reply
			}
		}
		kv.term=curTerm
	}
	kv.uncommitOpMap[op.Id]=op
	kv.mu.Unlock()

	kv.PrintLog("waiting Get request to reach agreement,id=%d",op.Id)

	*reply=(<-replyCh).(GetReply)
	kv.PrintLog("Get request return, id=%d isWrongLeader=%t",op.Id,reply.WrongLeader)
}

func (kv *RaftKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	var op Op
	var isLeader bool
	var curTerm int
	var replyCh chan interface{}

	replyCh=make(chan interface{})
	op.Id=args.Id
	op.Key=args.Key
	op.Value=args.Value
	op.Type=args.Op
	op.replyCh=replyCh
	op.LeaderId=kv.me

	kv.PrintLog("recieve PutAppend request from client, Key=%s, Value=%s, id=%d",args.Key,args.Value,args.Id)

	kv.mu.Lock()
	if _,curTerm,isLeader=kv.rf.Start(op);!isLeader{
		reply.WrongLeader=true
		//kv.PrintLog("receive PutAppend request from client, not leader, id=%d",op.Id)
		kv.mu.Unlock()
		return
	}
	if curTerm!=kv.term{
		for _,op:=range kv.uncommitOpMap{
			if op.Type=="Get"{
				var reply GetReply
				reply.WrongLeader=true
				op.replyCh<-reply
			}else{
				var reply PutAppendReply
				reply.WrongLeader=true
				op.replyCh<-reply
			}
		}
		kv.term=curTerm
	}
	kv.uncommitOpMap[op.Id]=op
	kv.mu.Unlock()
	kv.PrintLog("waiting PutAppend request to reach agreement, id=%d",op.Id)

	*reply=(<-replyCh).(PutAppendReply)
	kv.PrintLog("PutAppend request return, id=%d, isWrongLeader=%t",op.Id,reply.WrongLeader)
}

func (kv *RaftKV) ApplyRoutine(){
	for{
		select{
		case applyMsg:=<-kv.applyCh:
			op:=(applyMsg.Command).(Op)
			kv.PrintLog("raft apply index%d", applyMsg.Index)
			switch opType := op.Type; opType{
			case "Get":
				kv.mu.Lock()
				if kv.isDead {
					kv.mu.Unlock()
					return
				}
				curTerm,_:=kv.rf.GetState()
				if curTerm!=kv.term{
					for _, op := range kv.uncommitOpMap {
						if op.Type == "Get" {
							var reply GetReply
							reply.WrongLeader = true
							op.replyCh <- reply
						} else {
							var reply PutAppendReply
							reply.WrongLeader = true
							op.replyCh <- reply
						}
					}
					kv.term = curTerm
				}
				if _,ok:=kv.uncommitOpMap[op.Id];!ok{
					kv.mu.Unlock()
					continue
				}
				var reply GetReply
				var ok bool
				reply.Value, ok = kv.kvdb.Get(op.Key)
				if ok {
					reply.Err = OK
				} else {
					reply.Err = ErrNoKey
				}
				reply.WrongLeader = false
				op.replyCh <- reply
				delete(kv.uncommitOpMap,op.Id)
				kv.mu.Unlock()
			case "Put":
				fallthrough
			case "Append":
				kv.mu.Lock()
				if kv.isDead {
					kv.mu.Unlock()
					return
				}
				//duplicate detection
				isDupOp := false
				for _, id := range kv.commitOpList {
					if id == op.Id {
						isDupOp = true
						break
					}
				}

				if !isDupOp {
					if op.Type == "Put" {
						kv.kvdb.Put(op.Key, op.Value)
					} else {
						kv.kvdb.Append(op.Key, op.Value)
					}

				}
				kv.commitOpList = append(kv.commitOpList, op.Id)
				curTerm,_:=kv.rf.GetState()
				if curTerm!=kv.term{
					for _, op := range kv.uncommitOpMap {
						if op.Type == "Get" {
							var reply GetReply
							reply.WrongLeader = true
							op.replyCh <- reply
						} else {
							var reply PutAppendReply
							reply.WrongLeader = true
							op.replyCh <- reply
						}
					}
					kv.term = curTerm
				}
				if _,ok:=kv.uncommitOpMap[op.Id];!ok{
					kv.mu.Unlock()
					continue
				}
				var reply PutAppendReply
				reply.WrongLeader = false
				reply.Err = OK
				//kv.PrintLog("I am leader and apply request, id=%d",op.Id)

				op.replyCh <- reply
				delete(kv.uncommitOpMap,op.Id)
				kv.mu.Unlock()
			default:
				kv.mu.Lock()
				curTerm, _ := kv.rf.GetState()
				if curTerm != kv.term {
					for _, op := range kv.uncommitOpMap {
						if op.Type == "Get" {
							var reply GetReply
							reply.WrongLeader = true
							op.replyCh <- reply
						} else {
							var reply PutAppendReply
							reply.WrongLeader = true
							op.replyCh <- reply
						}
					}
					kv.term = curTerm
				}
				kv.mu.Unlock()
			}
		}
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
	kv.uncommitOpMap=make(map[int64]Op)
	kv.term,_=kv.rf.GetState()
	//kv.idxOpMap=make(map[int]Op)
	go kv.ApplyRoutine()

	return kv
}
