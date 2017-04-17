package raftkv

type KVDatabase struct{
	kv map[string]string
}


func (kvDB *KVDatabase) Get(key string) string{
	value,ok:=kvDB.kv[key]
	if ok{
		return value
	}else{
		return ""
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

func Make() *KVDatabase{
	kvDB:=&KVDatabase{}
	kvDB.kv=make(map[string]string)

	return kvDB
}
