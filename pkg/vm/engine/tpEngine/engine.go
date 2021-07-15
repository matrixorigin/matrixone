package tpEngine

import (
	"bytes"
	"fmt"
	"math"
	"matrixone/pkg/vm/engine"
	"matrixone/pkg/vm/engine/aoe/dist"
	"matrixone/pkg/vm/process"
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
		dbs: make(map[string]*tableDatabasesRow),
		recyclingDb: make(map[string]*tableDatabasesRow),
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

//database exists in the engine or not?
func (te *tpEngine)hasDatabase(name string) bool {
	_,ok := te.dbs[name]
	return ok
}

//save database
func (te *tpEngine) saveDatabase(name string, meta *tableDatabasesRow) {
	te.dbs[name] = meta
}

//remove database from the engine, put it into the recycling.
func (te *tpEngine)removeDatabase(name string) {
	meta,ok := te.dbs[name]
	if !ok{
		return
	}

	delete(te.dbs,name)
	te.recyclingDb[name] = meta
}

//async recycle database
func (te *tpEngine)recycleDatabases(){
	//TODO:
}


//unsafe in multi-thread
func (te *tpEngine) getNextDatabaseNo() uint64 {
	n := te.nextDbNo
	te.nextDbNo++
	return n
}

//unsafe in multi-thread
func (te *tpEngine) getNextTableNo() uint64 {
	n := te.nextTableNo
	te.nextTableNo++
	return n
}

//unsafe in multi-thread
func (te *tpEngine) loadDatabaseList() error {
	//if te.dbs == nil || len(te.dbs) == 0 {
		prefix := NewTpTableKey(stringKey(tpEngineName),
			uint64Key(0),uint64Key(TABLE_DATABASES_ID),uint64Key(0),
			nil,nil)
		prefix_key := prefix.encode(nil)
		prefix_key = append(prefix_key,tpEngineSlash)
		value,err := te.kv.PrefixScan(prefix_key, math.MaxUint64)
		if err != nil {
			return err
		}

		for i := 0; i < len(value); i += 2 {
			k := value[i]
			v := value[i+1]
			if bytes.HasPrefix(k,prefix_key) {
				db := string(k[len(prefix_key):])
				if !te.hasDatabase(db){
					dbRow := &tableDatabasesRow{}
					_,err = dbRow.decode(v)
					if err != nil {
						return err
					}
					te.saveDatabase(db,dbRow)
				}
			}

			//fmt.Printf("+++> %s\n",string(v))
		}
	//}
	return nil
}

/**
load database meta info, and save into the engine map.
 */
//unsafe in multi-thread
func (te *tpEngine) loadDatabase(db string) error {
	db_skey := NewTpTableKey(stringKey(tpEngineName),
		uint64Key(0),uint64Key(TABLE_DATABASES_ID),uint64Key(0),
		[]tpPrimaryKey{stringKey(db)},nil)
	db_key := db_skey.encode(nil)
	value,err := te.kv.Get(db_key)
	if err != nil {
		return err
	}

	dbRow := &tableDatabasesRow{}
	_,err = dbRow.decode(value)
	if err!=nil {
		return err
	}

	//save into
	te.saveDatabase(db,dbRow)

	return nil
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
	row := &tpTupleImpl{}
	for i:=0;i < len(kvs);i += 2 {
		key := bytes2string(kvs[i])
		_,err = row.decode(kvs[i+1])
		if err!= nil {
			fmt.Printf("===>%v %v \n",key,kvs[i+1])
		}else{
			fmt.Printf("===>%v %v \n",key,row)
		}

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
		[]tpPrimaryKey{stringKey(TABLE_META1_PRIMARY_KEY)},nil)
	meta_key := meta_skey.encode(nil)
	value,err := te.kv.Get(meta_key)
	if err != nil {
		return err
	}

	if value == nil || len(value) == 0 {
		return fmt.Errorf("db0 meta1 does not have pk")
	}

	fmt.Printf("((( %v\n",value)
	row := &tpTupleImpl{}
	_,err = row.decode(value)
	if err != nil{
		return err
	}

	te.nextDbNo = row.fields[1].(uint64)
	te.nextTableNo = row.fields[2].(uint64)

	return nil
}

//store table meta1 of the database 0
func (te *tpEngine) storeDatabase0Meta1() error {
	meta_skey := NewTpTableKey(stringKey(tpEngineName),
		uint64Key(0),uint64Key(TABLE_META1_ID),uint64Key(0),
		[]tpPrimaryKey{stringKey(TABLE_META1_PRIMARY_KEY)},nil)
	meta_key := meta_skey.encode(nil)
	metaRow := NewTpTupleImpl(TABLE_META1_PRIMARY_KEY,uint64(te.nextDbNo),uint64(te.nextTableNo))
	meta_val := metaRow.encode(nil)
	err := te.kv.Set(meta_key,meta_val)
	if err != nil {
		return err
	}

	return nil
}

/*
create the internal hierarchy for the database
*/
func (te *tpEngine)initDatabaseInternalHierarchy(dbname string, dbid uint64) error {
	/*
		step 1 :
		Databases
		------------------------
		0          1        2
		database   id       dbschema
		(primary)
		------------------------
		db0        0        "create database info"
	*/

	engDb0_skey := NewTpTableKey(stringKey(tpEngineName),
		uint64Key(0),uint64Key(TABLE_DATABASES_ID),uint64Key(0),
		[]tpPrimaryKey{stringKey(dbname)},nil)
	engDb0_key := engDb0_skey.encode(nil)
	dbrow := NewTableDatabasesRow(dbname,dbid,"create database info")
	engVal := dbrow.encode(nil)
	err := te.kv.Set(engDb0_key,engVal)
	if err != nil {
		return err
	}


	/*
		step 2 : default tables in the database
		Tables
		-----------------------
		0         1        2
		table     id       tableschema
		(primary)
		-----------------
		Databases 0        "create table info"
		Tables    1        "create table info"
		Indexes   2        "create table info"
		Meta1     3        "create table info"
		Meta2     4        "create table info"
	*/
	tableNames := [LAST_TABLE_ID+1]string{
		TABLE_DATABASES_NAME,
		TABLE_TABLES_NAME,
		TABLE_INDEXES_NAME,
		TABLE_META1_NAME,
		TABLE_META2_NAME,
		TABLE_VIEWS_NAME,
	}

	for i,tn := range tableNames {
		tab_skey := NewTpTableKey(stringKey(tpEngineName),
			uint64Key(dbid),uint64Key(TABLE_TABLES_ID),uint64Key(0),
			[]tpPrimaryKey{stringKey(tn)},nil)
		tab_key := tab_skey.encode(nil)
		tabRow := NewTpTupleImpl(tn,uint64(i),"create table info")
		tab_value := tabRow.encode(nil)
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
			0          1           2       3
			tableId    indexName   id      indexschema
			(  primary  key    )
			--------------------------
			0          primary     0       "create index info"
			1          primary     0       "create index info"
			2          primary     0       "create index info"
			3          primary     0       "create index info"
			4          primary     0       "create index info"

		*/
		idx_skey := NewTpTableKey(stringKey(tpEngineName),
			uint64Key(dbid),uint64Key(TABLE_INDEXES_ID),uint64Key(0),
			[]tpPrimaryKey{uint64Key(i),stringKey(TABLE_INDEXES_PRIMARY_KEY_PART2)},nil)
		idx_key := idx_skey.encode(nil)
		idxRow := NewTpTupleImpl(uint64(i),TABLE_INDEXES_PRIMARY_KEY_PART2,uint64(0),"create index info")
		idx_val := idxRow.encode(nil)
		err = te.kv.Set(idx_key,idx_val)
		if err != nil {
			return err
		}

		/*
				step 5 : table meta2 (merge with "tables")
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
		meta2Row := NewTpTupleImpl(uint64(i),uint64(1))
		meta2_val := meta2Row.encode(nil)
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
		pk                 1                LAST_TABLE_ID + 1
	*/
	meta_skey := NewTpTableKey(stringKey(tpEngineName),
		uint64Key(dbid),uint64Key(TABLE_META1_ID),uint64Key(0),
		[]tpPrimaryKey{stringKey(TABLE_META1_PRIMARY_KEY)},nil)
	meta_key := meta_skey.encode(nil)
	metaRow := NewTpTupleImpl(TABLE_META1_PRIMARY_KEY,uint64(1),uint64(LAST_TABLE_ID+1))
	meta_s_value := metaRow.encode(nil)
	//fmt.Printf("[[[ %v\n",meta_s_value)
	err = te.kv.Set(meta_key,meta_s_value)
	if err != nil {
		return err
	}

	/*
	step 6 : table views
	Views
	-------------------------------------
	0             1         2
	viewname      viewId    viewschema
	*/

	return nil
}

//Create database
func (te *tpEngine) Create(db string, et int) error {
	//TODO:add transaction protection
	te.rwlock.Lock()
	defer te.rwlock.Unlock()

	//step 1: get database list in the engine
	err := te.loadDatabaseList()
	if err != nil {
		return err
	}

	//step 2: check if the db has existed
	if te.hasDatabase(db){
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

	//step 6 : load and save database
	err = te.loadDatabase(db)
	if err != nil {
		return err
	}

	return nil
}

func (te *tpEngine) Delete(db string) error {
	//TODO: add transaction protection
	te.rwlock.Lock()
	defer te.rwlock.Unlock()

	//step 1: get database list
	err := te.loadDatabaseList()
	if err != nil {
		return err
	}

	//step 2: check if the db has existed
	if !te.hasDatabase(db) {
		return nil
	}

	//step 3: remove the db from the engine
	db_skey := NewTpTableKey(stringKey(te.engName),
		uint64Key(0),uint64Key(TABLE_DATABASES_ID),uint64Key(0),
		[]tpPrimaryKey{stringKey(db)},nil)
	db_key := db_skey.encode(nil)

	err = te.kv.Delete(db_key)
	if err != nil {
		return err
	}

	//step4: move the db into recycling query
	te.removeDatabase(db)

	return nil
}

func (te *tpEngine) Databases() []string {
	te.rwlock.Lock()
	defer te.rwlock.Unlock()

	err := te.loadDatabaseList()
	if err != nil {
		panic(err)
	}

	keys := make([]string,0, len(te.dbs))
	for k := range te.dbs {
		keys = append(keys,k)
	}

	return keys
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
