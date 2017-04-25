package raftkv

type KVDatabase struct{
	kv map[string]string
}


func (kvDB *KVDatabase) Get(key string) (string,bool){
	value,ok:=kvDB.kv[key]
	if ok{
		return value,ok
	}else{
		return "",ok
	}
}

func (kvDB *KVDatabase) Put(key string,value string){
	kvDB.kv[key]=value
}

func (kvDB *KVDatabase) Append(key string,value string){
	oldValue,ok:=kvDB.kv[key]
	if ok{
		kvDB.kv[key]=oldValue+value
	}else{
		kvDB.kv[key]=value
	}
}

func (kvDB *KVDatabase) Make(){
	kvDB.kv=make(map[string]string)
}
