package raftkv

type KVDatabase struct{
	Kv map[string]string
}


func (kvDB *KVDatabase) Get(key string) (string,bool){
	value,ok:=kvDB.Kv[key]
	if ok{
		return value,ok
	}else{
		return "",ok
	}
}

func (kvDB *KVDatabase) Put(key string,value string){
	kvDB.Kv[key]=value
}

func (kvDB *KVDatabase) Append(key string,value string){
	oldValue,ok:=kvDB.Kv[key]
	if ok{
		kvDB.Kv[key]=oldValue+value
	}else{
		kvDB.Kv[key]=value
	}
}

func (kvDB *KVDatabase) Make(){
	kvDB.Kv=make(map[string]string)
}
