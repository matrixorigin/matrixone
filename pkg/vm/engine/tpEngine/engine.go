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
merge multiple schemas by order into the one
 */
func mergeTpSchema(schs ...*tpSchema)*tpSchema{
	t := NewTpSchema()
	for _,sch := range schs {
		t.colTypes = append(t.colTypes,sch.colTypes...)
		t.used = append(t.used,sch.used...)
	}
	return t
}

/**
encode prefix
 */
func (ttk *tpTableKey) encodePrefix(data []byte) []byte {
	return encodeKeysWithSchema(data,ttk.prefixSchema,
		ttk.engine,
		ttk.dbId,
		ttk.tableId,
		ttk.indexId,
	)
}

func (ttk *tpTableKey) decodePrefix(data []byte) ([]byte,error) {
	data1,prefixs,err := decodeKeys(data,ttk.prefixSchema)
	if err != nil {
		return nil, err
	}

	ttk.engine = prefixs[0].(string)
	ttk.dbId = prefixs[1].(uint64)
	ttk.tableId = prefixs[2].(uint64)
	ttk.indexId = prefixs[3].(uint64)
	return data1,nil
}

/**
encode primary keys
 */
func (ttk *tpTableKey) encodePrimaryKeys(data []byte)[]byte{
	if ttk.primarySchema == nil {
		return data
	}
	if ttk.primaries == nil  || len(ttk.primaries) != ttk.primarySchema.ColumnCount(){
		panic("missing primary keys or schema")
	}

	return encodeKeysWithSchema(data,ttk.primarySchema,ttk.primaries...)
}

func (ttk *tpTableKey) decodePrimaryKeys(data []byte)([]byte,error){
	if ttk.primarySchema == nil {
		return data,nil
	}
	data1,prims,err := decodeKeys(data,ttk.primarySchema)
	if err != nil {
		return nil, err
	}

	ttk.primaries = prims

	return data1, nil
}

/**
encode prefix and primary keys
*/
func (ttk *tpTableKey) encodePrefixAndPrimaryKeys(data []byte)[]byte{
	data = ttk.encodePrefix(data)

	data = ttk.encodePrimaryKeys(data)
	return  data
}

func (ttk *tpTableKey) decodePrefixAndPrimaryKeys(data []byte) ([]byte,error){
	data1,err := ttk.decodePrefix(data)
	if err != nil {
		return nil,err
	}

	data2,err := ttk.decodePrimaryKeys(data1)
	if err != nil {
		return nil, err
	}
	return data2,nil
}

/**
encode suffix keys
*/
func (ttk *tpTableKey) encodeSuffix(data []byte)[]byte{
	if ttk.suffixSchema == nil {
		return data
	}

	if ttk.suffix == nil || len(ttk.suffix) != ttk.suffixSchema.ColumnCount() {
		panic("missing suffix keys or schema")
	}

	return encodeKeysWithSchema(data,ttk.suffixSchema,ttk.suffix...)
}

func (ttk *tpTableKey) decodeSuffix(data []byte)([]byte,error){
	if ttk.suffixSchema == nil {
		return data, nil
	}
	data1,sufs,err := decodeKeys(data,ttk.suffixSchema)
	if err != nil {
		return nil,err
	}

	ttk.suffix = sufs

	return data1,nil
}

/**
encode table key into the comparable bytes
 */
func (ttk *tpTableKey) encode(data []byte)[]byte{
	data = ttk.encodePrefix(data)

	data = ttk.encodePrimaryKeys(data)

	data = ttk.encodeSuffix(data)
	return data
}

/**
decode bytes into tptablekey
 */
func (ttk *tpTableKey) decode(data []byte)([]byte,error){
	data1,err := ttk.decodePrefix(data)
	if err != nil {
		return nil,err
	}

	data2,err := ttk.decodePrimaryKeys(data1)
	if err != nil {
		return nil, err
	}

	data3,err := ttk.decodeSuffix(data2)
	if err != nil {
		return nil, err
	}
	return data3,nil
}

func (ttk *tpTableKey) SetPrimarySchema(sch *tpSchema){
	ttk.primarySchema = sch
}

func (ttk *tpTableKey) SetSuffixSchema(sch *tpSchema){
	ttk.suffixSchema = sch
}

func (ttk *tpTableKey) isPrefixEqualTo(o *tpTableKey) bool {
	if o == nil {
		return false
	}

	return ttk.engine == ttk.engine &&
		ttk.dbId == ttk.dbId &&
		ttk.tableId == ttk.tableId &&
		ttk.indexId == ttk.indexId
}

func (ttk *tpTableKey) String() string {
	return fmt.Sprintf("%s-%d-%d-%d-[%v]-[%v]",
		ttk.engine,ttk.dbId,ttk.tableId,ttk.indexId,
		ttk.primaries,
		ttk.suffix)
}

func NewTpEngine(eng string,kv dist.Storage,proc *process.Process)*tpEngine {
	//TODO:load persisted status
	return &tpEngine{
		engName:     eng,
		kv:          kv,
		proc:        proc,
		nextDbNo:    0, //TODO:fix persistence
		nextTableNo: 0, //TODO:fix persistence
		dbs: make(map[string]*tpTupleImpl),
		recyclingDb: make(map[string]*tpTupleImpl),
	}
}

//database exists in the engine or not?
func (te *tpEngine)hasDatabase(name string) bool {
	_,ok := te.dbs[name]
	return ok
}

//save database
func (te *tpEngine) saveDatabase(name string, meta *tpTupleImpl) {
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
		prefix := NewTpTableKey(tpEngineName,
			tpEngineDatabase0Id, TABLE_DATABASES_ID, 0,
			nil,
			nil,
			nil,
			nil)
		prefix_key := prefix.encodePrefix(nil)
		value,err := te.kv.PrefixScan(prefix_key, math.MaxUint64)
		if err != nil {
			return err
		}

		for i := 0; i < len(value); i += 2 {
			k := value[i]
			v := value[i+1]
			if bytes.HasPrefix(k,prefix_key) {
				_,keys,err := decodeKeys(k[len(prefix_key):],TABLE_DATABASES_PRIMARY_KEY_SCHEMA)
				if err != nil {
					return err
				}
				db := keys[0].(string)
				fmt.Printf("+++> db: %v \n",db)
				if !te.hasDatabase(db){
					dbRow := &tpTupleImpl{}
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
	db_skey := NewTpTableKey(tpEngineName,
		tpEngineDatabase0Id, TABLE_DATABASES_ID, 0,
		TABLE_DATABASES_PRIMARY_KEY_SCHEMA,
		[]interface{}{db},
		nil,
		nil)
	db_key := db_skey.encode(nil)
	value,err := te.kv.Get(db_key)
	if err != nil {
		return err
	}

	dbRow := &tpTupleImpl{}
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
	prefix := NewTpTableKey(te.engName,
		db, tab, idx,
		nil,
		nil,
		nil,
		nil)
	prefix_key := prefix.encodePrefix(nil)
	kvs,err := te.kv.PrefixScan(prefix_key,math.MaxUint64)
	if err != nil {
		return err
	}
	fmt.Printf("===>db %v table %v idx %v\n",db,tab,idx)
	var tbk *tpSchema = nil
	switch tab {
	case TABLE_DATABASES_ID:
		tbk = mergeTpSchema(TP_ENGINE_PREFIX_KEY,TABLE_DATABASES_PRIMARY_KEY_SCHEMA)
	case TABLE_TABLES_ID:
		tbk = mergeTpSchema(TP_ENGINE_PREFIX_KEY,TABLE_TABLES_PRIMARY_KEY_SCHEMA)
	case TABLE_INDEXES_ID:
		tbk = mergeTpSchema(TP_ENGINE_PREFIX_KEY,TABLE_INDEXES_PRIMARY_KEY_SCHEMA)
	case TABLE_META1_ID:
		tbk = mergeTpSchema(TP_ENGINE_PREFIX_KEY,TABLE_META1_PRIMARY_KEY_SCHEMA)
	case TABLE_META2_ID:
		tbk = mergeTpSchema(TP_ENGINE_PREFIX_KEY,TABLE_META2_PRIMARY_KEY_SCHEMA)
	case TABLE_VIEWS_ID:
		tbk = mergeTpSchema(TP_ENGINE_PREFIX_KEY,TABLE_VIEWS_PRIMARY_KEY_SCHEMA)
	default:
		panic(fmt.Errorf("unsupported table key %v",tab))
	}
	row := &tpTupleImpl{}
	for i:=0;i < len(kvs);i += 2 {
		_,key, err := decodeKeys(kvs[i],tbk)
		if err != nil {
			return err
		}
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
	meta_skey := NewTpTableKey(tpEngineName,
		tpEngineDatabase0Id, TABLE_META1_ID, 0,
		TABLE_META1_PRIMARY_KEY_SCHEMA,
		[]interface{}{TABLE_META1_PRIMARY_KEY},
		nil,
		nil)
	meta_key := meta_skey.encode(nil)
	value,err := te.kv.Get(meta_key)
	if err != nil {
		return err
	}

	if value == nil || len(value) == 0 {
		return fmt.Errorf("db0 meta1 does not have pk")
	}

	row := &tpTupleImpl{}
	_,err = row.decode(value)
	if err != nil{
		return err
	}

	te.nextDbNo = row.fields[0].(uint64)
	te.nextTableNo = row.fields[1].(uint64)

	return nil
}

//store table meta1 of the database 0
func (te *tpEngine) storeDatabase0Meta1() error {
	meta_skey := NewTpTableKey(tpEngineName,
		tpEngineDatabase0Id, TABLE_META1_ID, 0,
		TABLE_META1_PRIMARY_KEY_SCHEMA,
		[]interface{}{TABLE_META1_PRIMARY_KEY},
		nil,
		nil)
	meta_key := meta_skey.encode(nil)
	metaRow := NewTpTupleImpl(TABLE_META1_REST_SCHEMA,
		uint64(te.nextDbNo), uint64(te.nextTableNo))
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
		0          1        2          3
		database   id       createInfo schema
		(primary)
		------------------------
		db0        0        "create database info"
	*/

	engDb0_skey := NewTpTableKey(tpEngineName,
		tpEngineDatabase0Id, TABLE_DATABASES_ID, uint64(0),
		TABLE_DATABASES_PRIMARY_KEY_SCHEMA,
		[]interface{}{dbname},
		nil,
		nil)
	engDb0_key := engDb0_skey.encode(nil)
	dbrow := NewTpTupleImpl(TABLE_DATABASES_REST_SCHEMA,
		dbid, "create database info","schema encode string")
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
		tab_skey := NewTpTableKey(tpEngineName,
			dbid, TABLE_TABLES_ID, uint64(0),
			TABLE_TABLES_PRIMARY_KEY_SCHEMA,
			[]interface{}{tn},
			nil,
			nil)
		tab_key := tab_skey.encode(nil)
		tabRow := NewTpTupleImpl(TABLE_TABLES_REST_SCHEMA,
			uint64(i), "create table info","schema encode info")
		tab_value := tabRow.encode(nil)
		fmt.Printf("///> %v %v \n",tab_skey,tabRow)
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
		idx_skey := NewTpTableKey(tpEngineName,
			dbid, TABLE_INDEXES_ID, uint64(0),
			TABLE_INDEXES_PRIMARY_KEY_SCHEMA,
			[]interface{}{uint64(i), TABLE_INDEXES_PRIMARY_KEY_PART2},
			nil,
			nil)
		idx_key := idx_skey.encode(nil)
		idxRow := NewTpTupleImpl(TABLE_INDEXES_REST_SCHEMA,
			uint64(0), "create index info","schema encode string")
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
		meta2_skey := NewTpTableKey(tpEngineName,
			dbid, TABLE_META2_ID, uint64(0),
			TABLE_META2_PRIMARY_KEY_SCHEMA,
			[]interface{}{uint64(i)},
			nil,
			nil)
		meta2_key := meta2_skey.encode(nil)
		meta2Row := NewTpTupleImpl(TABLE_META2_REST_SCHEMA,
			uint64(1))
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
	meta_skey := NewTpTableKey(tpEngineName,
		dbid, TABLE_META1_ID, uint64(0),
		TABLE_META1_PRIMARY_KEY_SCHEMA,
		[]interface{}{TABLE_META1_PRIMARY_KEY},
		nil,
		nil)
	meta_key := meta_skey.encode(nil)
	metaRow := NewTpTupleImpl(TABLE_META1_REST_SCHEMA,
		uint64(1), uint64(LAST_TABLE_ID+1))
	meta_s_value := metaRow.encode(nil)
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

/**
init the engine with database0 for internal usage.
create the database 0 if not exsits
 */
func (te *tpEngine) Init() error {
	//TODO:add transaction protection
	te.rwlock.Lock()
	defer te.rwlock.Unlock()

	//step 1: get database list in the engine
	err := te.loadDatabaseList()
	if err != nil {
		return err
	}

	//step 2: check if the db has existed
	if te.hasDatabase(tpEngineDatabase0){
		return nil
	}

	//step 3: create database0
	//step 4: put the db in the engine with a new id
	err = te.initDatabaseInternalHierarchy(tpEngineDatabase0,tpEngineDatabase0Id)
	if err != nil {
		return err
	}
	return nil
}

//Create database
func (te *tpEngine) Create(db string,tp int) error {
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
	db_skey := NewTpTableKey(te.engName,
		tpEngineDatabase0Id, TABLE_DATABASES_ID, 0,
		TABLE_DATABASES_PRIMARY_KEY_SCHEMA,
		[]interface{}{db},
		nil,
		nil)
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
