package shardkv

type Shard struct {
	Id int
	Kv map[string]string
}

func (sd *Shard) Get(key string) (string,bool){
	value,ok:=sd.Kv[key]
	if ok{
		return value,ok
	}else{
		return "",ok
	}
}

func (sd *Shard) Put(key string,value string){
	sd.Kv[key]=value
}

func (sd *Shard) Append(key string,value string){
	oldValue,ok:=sd.Kv[key]
	if ok{
		sd.Kv[key]=oldValue+value
	}else{
		sd.Kv[key]=value
	}
}

func (sd *Shard) Make(id int){
	sd.Id=id
	sd.Kv=make(map[string]string)
}

func CopyShard(sd1 *Shard,sd2 *Shard){
	sd1.Id=sd2.Id
	sd1.Kv=make(map[string]string)
	for key,value:=range sd2.Kv{
		sd1.Kv[key]=value
	}
}

