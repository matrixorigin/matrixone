package tpEngine

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"go/constant"
	"math"
	"matrixone/pkg/vm/engine"
	"matrixone/pkg/vm/engine/aoe/dist"
	"matrixone/pkg/vm/process"
)

//make engine name
func makeEngineName(name string)string {
	return "tp-"+name
}

func makeEngineMetaPrefix(e string) string {
	return fmt.Sprintf("tp-%s-meta-",e)
}

func makeEngineMetaDatabaseNoKey(e string) string {
	return fmt.Sprintf("%sdb-no",makeEngineMetaPrefix(e))
}

func makeEngineMetaSerialNoKey(e string) string {
	return fmt.Sprintf("%sserial-no",makeEngineMetaPrefix(e))
}

func makeEngineDatabasePrefix(e string) string {
	return fmt.Sprintf("tp-%s-db-",e)
}

func makeEngineDatabaseKey(e string,no uint64) string {
	return fmt.Sprintf("%s%d",makeEngineDatabasePrefix(e),no)
}

func makeEngineDatabaseNameKey(e string,db string) string {
	return fmt.Sprintf("%s%s",makeEngineDatabasePrefix(e),db)
}

func makeEngineSerial(e string,no uint64) string {
	return fmt.Sprintf("tp-%s-serial-%d",e,no)
}

func makeTablePrefix(e string,db,table,index uint64) string {
	return fmt.Sprintf("%s-%d-%d-%d-",makeEngineName(e),db,table,index)
}

func makeTableKey(e string,db,table,index uint64,pks string) string {
	return fmt.Sprintf("%s%s",makeTablePrefix(e,db,table,index),pks)
}

//make engineName + db no
func makeEngineWithDbNo(eng string,db int64)string{
	return fmt.Sprintf("tp-%s-no-%d",eng,db)
}

//make dbname + rel no
func makeDatabaseNameWithRelationNo(db string,rel int64)string  {
	return fmt.Sprintf("tp-%s-rel-%d",db,rel)
}

//make relation name + primary key
func makeRelationNameWithPrimaryKey(rel string, pk interface{}) string {
	return fmt.Sprintf("tp-%s-pk-%v",rel,pk)
}

/**
primary key type
 */
type stringKey string

func (sk stringKey) encode(data []byte) []byte {
	data = append(data,string2bytes(string(sk))...)
	return data
}

func (sk stringKey) String() string {
	return fmt.Sprintf("%s",string(sk))
}

type uint64Key uint64

func (uk uint64Key) encode(data []byte) []byte {
	//elementary method
	data = append(data,fmt.Sprintf("%d",uk)...)
	return data
}

func (uk uint64Key) String() string {
	return fmt.Sprintf("%v",uint64(uk))
}

/**
value type
 */
type stringValue string

func (sv stringValue) encodeValue(data []byte) []byte {
	data = append(data,TP_ENCODE_TYPE_STRING)
	return append(data,string2bytes(string(sv))...)
}

func (sv stringValue) String() string {
	return string(sv)
}

type uint64Value uint64

func (uv uint64Value) encodeValue(data []byte) []byte {
	v := uint64(uv)
	var tmp []byte = make([]byte,8)
	binary.BigEndian.PutUint64(tmp,v)
	//encode tage
	data = append(data,TP_ENCODE_TYPE_UINT64)
	return append(data,tmp...)
}

func (uv uint64Value) decodeValue(data []byte) {

}

func (uv uint64Value) String()string{
	return ""
}

func encodeValue(data []byte,args ...interface{})[]byte{
	for _,arg := range args{
		switch a := arg.(type) {
		case uint64:
			data = encodeUint64(data,a)
		case string:
			data = encodeString(data,a)
		default:
			panic(fmt.Errorf("unsupported value %v",a))
		}
	}
	return data
}

//length ?
func encodeString(data []byte,s string)[]byte{
	data = append(data,TP_ENCODE_TYPE_STRING)
	//add length
	data = append(data,encodeUint64(data,uint64(len(s)))...)
	return append(data,string2bytes(s)...)
}

func decodeString(data []byte)([]byte,string,error){
	nd,u,err := decodeUint64(data)
	if err != nil {
		return nil,"",err
	}
	return nd,bytes2string(data[8:8+u]),nil
}

func encodeUint64(data []byte,u uint64)[]byte{
	var tmp []byte = make([]byte,8)
	binary.BigEndian.PutUint64(tmp,u)
	//encode tage
	data = append(data,TP_ENCODE_TYPE_UINT64)
	return append(data,tmp...)
}

func decodeUint64(data []byte)([]byte,uint64,error){
	return data[8:],binary.BigEndian.Uint64(data),nil
}

func decodeValue(data []byte)([]byte,constant.Value,error) {
	if len(data) < 1 {
		return nil, nil, fmt.Errorf("invalid encoded bytes")
	}

	switch data[0] {
	case TP_ENCODE_TYPE_UINT64:
		nd,u,err := decodeUint64(data[1:])
		if err!= nil{
			return nil, nil, err
		}
		return nd,constant.MakeUint64(u),nil
	case TP_ENCODE_TYPE_STRING:
		nd,s,err := decodeString(data[1:])
		if err!= nil{
			return nil,nil,err
		}
		return nd,constant.MakeString(s),nil
	default:
		panic(fmt.Errorf("unsupported encode type %v",data[0]))
	}
}

const (
	TABLE_DATABASES_ID uint64 = 0
	TABLE_DATABASES_NAME string = "databases"
	TABLE_TABLES_ID uint64 = 1
	TABLE_TABLES_NAME string = "tables"
	TABLE_INDEXES_ID uint64 = 2
	TABLE_INDEXES_NAME string = "indexes"
	TABLE_META1_ID uint64 = 3
	TABLE_META1_NAME string = "meta1"
	TABLE_META1_PRIMARY_KEY string = "pk"

	TABLE_META2_ID uint64 = 4
	TABLE_META2_NAME string = "meta2"

	LAST_TABLE_ID uint64 = TABLE_META2_ID
)

/**
encode table key into the comparable bytes
 */
func (ttk *tpTableKey) encode(data []byte)[]byte{
	//elementary method
	//"tp-" prefix
	data = append(data,string2bytes("tp")...)

	//engine name
	data = append(data,tpEngineSlash)
	data = ttk.engine.encode(data)

	//dbid tableod indexid
	data = append(data,tpEngineSlash)
	data = ttk.dbId.encode(data)

	data = append(data,tpEngineSlash)
	data = ttk.tableId.encode(data)

	data = append(data,tpEngineSlash)
	data = ttk.indexId.encode(data)

	for _,pr := range ttk.primaries {
		data = append(data,tpEngineSlash)
		data = pr.encode(data)
	}

	for _,su := range ttk.suffix {
		data = append(data,tpEngineSlash)
		data = su.encode(data)
	}
	return data
}

func (ttk *tpTableKey) String() string {
	var primas string = "["
	for _,pr := range ttk.primaries {
		primas += pr.String()+","
	}
	primas += "]"

	var sufs string = "["
	for _,su := range ttk.suffix {
		sufs += su.String()+","
	}
	sufs += "]"

	return fmt.Sprintf("tp-%s-%d-%d-%d-%s-%s",ttk.engine,ttk.dbId,ttk.tableId,ttk.indexId,primas,sufs)
}

func NewTpTableKey(e tpPrimaryKey,db,table,index tpPrimaryKey,prims []tpPrimaryKey,sufs []tpPrimaryKey) *tpTableKey{
	return &tpTableKey{
		engine:    e,
		dbId:      db,
		tableId:   table,
		indexId:   index,
		primaries: prims,
		suffix:    sufs,
	}
}

func NewTpEngine(eng string,kv dist.Storage,proc *process.Process)*tpEngine {
	//TODO:load persisted status
	return &tpEngine{
		engName:     eng,
		kv:          kv,
		proc:        proc,
		nextDbNo:    0, //TODO:fix persistence
		nextTableNo: 0, //TODO:fix persistence
	}
}

func string2bytes(s string)[]byte{
	return []byte(s)
}

func bytes2string(bts []byte)string{
	return string(bts)
}

/**
check if x in a slice
*/
func isInSlice(x string,arr []string) bool {
	for _,y := range arr{
		if x == y {
			return true
		}
	}
	return false
}

//remove a string from array, return a new slice
func removeFromSlice(x string,arr []string)[]string {
	var i int
	var y string
	for i,y = range arr{
		if x == y {
			break
		}
	}

	if i < len(arr) {
		return append(arr[:i],arr[i+1:]...)
	}
	return arr
}

/*
create the internal hierarchy for the database
 */
func (te *tpEngine)initDatabaseInternalHierarchy(dbname string, dbid uint64) error {
	/*
	step 1 :
	Databases
	-------------
	0          1
	database   id
	(primary)
	-------------
	db0        0
	*/

	engDb0_skey := NewTpTableKey(stringKey(tpEngineName),
		uint64Key(0),uint64Key(TABLE_DATABASES_ID),uint64Key(0),
		[]tpPrimaryKey{stringKey(dbname)},nil)
	engDb0_key := engDb0_skey.encode(nil)
	engVal := encodeValue(nil,dbid)
	err := te.kv.Set(engDb0_key,engVal)
	if err != nil {
		return err
	}


	/*
	step 2 : default tables in the database
	Tables
	-----------------
	0         1
	table     id
	(primary)
	-----------------
	Databases     0
	Tables   1
	Indexes     2
	Meta1 3
	Meta2 4
	 */
	tableNames := [LAST_TABLE_ID+1]string{
		TABLE_DATABASES_NAME,
		TABLE_TABLES_NAME,
		TABLE_INDEXES_NAME,
		TABLE_META1_NAME,
		TABLE_META2_NAME,
	}

	for i,tn := range tableNames {
		tab_skey := NewTpTableKey(stringKey(tpEngineName),
			uint64Key(dbid),uint64Key(TABLE_TABLES_ID),uint64Key(0),
			[]tpPrimaryKey{stringKey(tn)},nil)
		tab_key := tab_skey.encode(nil)
		tab_value := encodeValue(nil,uint64(i))
		err = te.kv.Set(tab_key, tab_value)
		if err != nil {
			return err
		}

		/*
		create default indexes for all tables
		 */
		/*
			step 3 : default indexes on the table
			Indexes
			-------------------------
			0          1           2
			tableId    indexName   id
			(  primary  key    )
			--------------------------
			0          primary     0
			1          primary     0
			2          primary     0
			3          primary     0
			4          primary     0

		*/
		idx_skey := NewTpTableKey(stringKey(tpEngineName),
			uint64Key(dbid),uint64Key(TABLE_INDEXES_ID),uint64Key(0),
			[]tpPrimaryKey{uint64Key(i),stringKey("primary")},nil)
		idx_key := idx_skey.encode(nil)
		idx_val := encodeValue(nil,uint64(0))
		err = te.kv.Set(idx_key,idx_val)
		if err != nil {
			return err
		}

		/*
				step 5 : table meta2
				Meta1
				------------------------------------------
				0                1
				tableid          nextIndexId
				(primary key)
				------------------------------------------
				0                1
				1                1
			    2                1
			    3                1
			    4                1
		*/
		meta2_skey := NewTpTableKey(stringKey(tpEngineName),
			uint64Key(dbid),uint64Key(TABLE_META2_ID),uint64Key(0),
			[]tpPrimaryKey{uint64Key(i)},nil)
		meta2_key := meta2_skey.encode(nil)
		meta2_val := encodeValue(nil,uint64(1))
		err = te.kv.Set(meta2_key,meta2_val)
		if err != nil {
			return err
		}
	}

	/*
	step 4 : table meta1
	Meta1
	------------------------------------------
	0                1                2
	rowid            nextDatabaseId   nextTableId
	------------------------------------------
	pk                 1                5
	*/
	meta_skey := NewTpTableKey(stringKey(tpEngineName),
		uint64Key(dbid),uint64Key(TABLE_META1_ID),uint64Key(0),
		[]tpPrimaryKey{stringKey("pk")},nil)
	meta_key := meta_skey.encode(nil)
	meta_s_value := encodeValue(nil,uint64(1),uint64(LAST_TABLE_ID+1))
	fmt.Printf("[[[ %v\n",meta_s_value)
	err = te.kv.Set(meta_key,meta_s_value)
	if err != nil {
		return err
	}

	return nil
}

//unsafe in multi-thread
func (te *tpEngine) getNextDatabaseNo() uint64 {
	n := te.nextDbNo
	te.nextDbNo++
	return n
}

//unsafe in multi-thread
func (te *tpEngine) getNextSerialNo() uint64 {
	n := te.nextTableNo
	te.nextTableNo++
	return n
}

//unsafe in multi-thread
func (te *tpEngine) getDatabaseList() ([]string, error) {
	//if te.dbs == nil || len(te.dbs) == 0 {
		prefix := NewTpTableKey(stringKey(tpEngineName),uint64Key(0),uint64Key(0),uint64Key(0),nil,nil)
		prefix_key := prefix.encode(nil)
		prefix_key = append(prefix_key,tpEngineSlash)
		value,err := te.kv.PrefixKeys(prefix_key, math.MaxUint64)
		if err != nil {
			return nil,err
		}

		for _,v := range value {
			if bytes.HasPrefix(v,prefix_key) {
				db := string(v[len(prefix_key):])
				if !isInSlice(db,te.dbs){
					te.dbs = append(te.dbs,db)
				}
			}

			//fmt.Printf("+++> %s\n",string(v))
		}
	//}
	return te.dbs,nil
}

//unsafe in multi-thread
func (te *tpEngine) putDatabaseIntoDatabaseList(db string) {
	//te.dbs = append(te.dbs,db)
}

//unsafe in multi-thread
func (te *tpEngine) putDatabaseIntoRecyclingList(db string) {
	//te.recyclingDb = append(te.recyclingDb,db)
}

/*
get all key-value pairs in a table
 */
//unsafe in multi-thread
func (te *tpEngine) getAllKvInTable(db,tab,idx uint64) error {
	prefix := NewTpTableKey(stringKey(te.engName),uint64Key(db),uint64Key(tab),uint64Key(idx),nil,nil)
	prefix_key := prefix.encode(nil)
	kvs,err := te.kv.PrefixScan(prefix_key,math.MaxUint64)
	if err != nil {
		return err
	}
	fmt.Printf("===>db %v table %v idx %v\n",db,tab,idx)
	for i:=0;i < len(kvs);i += 2 {
		key := bytes2string(kvs[i])
		//value := bytes2string(kvs[i+1])
		fmt.Printf("===>%v %v \n",key,kvs[i+1])
	}
	return nil
}

//for test
func (te *tpEngine) setDefaultDatabase0Meta1(){
	te.nextDbNo = 1
	te.nextTableNo	= LAST_TABLE_ID + 1
}

//load table meta1 of the database 0
func (te *tpEngine) loadDatabase0Meta1() error {
	meta_skey := NewTpTableKey(stringKey(tpEngineName),
		uint64Key(0),uint64Key(TABLE_META1_ID),uint64Key(0),
		[]tpPrimaryKey{stringKey("pk")},nil)
	meta_key := meta_skey.encode(nil)
	value,err := te.kv.Get(meta_key)
	if err != nil {
		return err
	}

	if value == nil || len(value) == 0 {
		return fmt.Errorf("db0 meta1 does not have pk")
	}

	fmt.Printf("((( %v\n",value)
	data,v1,err := decodeValue(value)
	if err != nil {
		return err
	}

	data,v2,err := decodeValue(data)
	if err != nil {
		return err
	}

	vv1,ok := constant.Uint64Val(v1)
	if !ok {
		return fmt.Errorf("extract v1 failed.")
	}
	te.nextDbNo = vv1

	vv2,ok := constant.Uint64Val(v2)
	if !ok {
		return fmt.Errorf("extract v2 failed.")
	}
	te.nextTableNo = vv2

	return nil
}

//store table meta1 of the database 0
func (te *tpEngine) storeDatabase0Meta1() error {
	meta_skey := NewTpTableKey(stringKey(tpEngineName),
		uint64Key(0),uint64Key(TABLE_META1_ID),uint64Key(0),
		[]tpPrimaryKey{stringKey("pk")},nil)
	meta_key := meta_skey.encode(nil)
	meta_val := encodeValue(nil,uint64(te.nextDbNo),uint64(te.nextTableNo))
	err := te.kv.Set(meta_key,meta_val)
	if err != nil {
		return err
	}

	return nil
}

//Create database
func (te *tpEngine) Create(db string, et int) error {
	//TODO:add transaction protection
	te.rwlock.Lock()
	defer te.rwlock.Unlock()

	//step 1: get database list in the engine
	dbs,err := te.getDatabaseList()
	if err != nil {
		return err
	}

	//step 2: check if the db has existed
	if isInSlice(db,dbs) {
		return fmt.Errorf("database %s has exists",db)
	}

	//step3: load db0 meta1 (nextDatabaseId)
	err = te.loadDatabase0Meta1()
	if err != nil {
		te.setDefaultDatabase0Meta1()
	}

	//step 4: put the db in the engine with a new id
	err = te.initDatabaseInternalHierarchy(db,te.getNextDatabaseNo())
	if err != nil {
		return err
	}

	//step 5: store db0 meta1
	err = te.storeDatabase0Meta1()
	if err != nil {
		return err
	}
	te.putDatabaseIntoDatabaseList(db)
	return nil
}

func (te *tpEngine) Delete(db string) error {
	//TODO: add transaction protection
	te.rwlock.Lock()
	defer te.rwlock.Unlock()

	//step 1: get database list
	dbs,err := te.getDatabaseList()
	if err != nil {
		return err
	}

	//step 2: check if the db has existed
	if !isInSlice(db,dbs) {
		return nil
	}

	//step 3: remove the db from the engine
	db_skey := NewTpTableKey(stringKey(te.engName),uint64Key(0),uint64Key(0),uint64Key(0),nil,nil)
	db_key := db_skey.encode(nil)

	err = te.kv.Delete(db_key)
	if err != nil {
		return err
	}

	te.dbs = removeFromSlice(db,te.dbs)

	//step4: put the db into recycling query
	te.putDatabaseIntoRecyclingList(db)

	return nil
}

func (te *tpEngine) Databases() []string {
	dbs,_ := te.getDatabaseList()
	return dbs
}

func (te *tpEngine) Database(db string) (engine.Database, error){
	dbs := te.Databases()
	if !isInSlice(db,dbs) {
		return nil, fmt.Errorf("database %v does not exist",db)
	}
	//TODO:create a database
	return nil, nil
}

func (te *tpEngine) Node(string) *engine.NodeInfo {
	return nil
}
