package tpEngine

import (
	"fmt"
	"math"
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

func NewTpEngine(eng string,kv dist.Storage,proc *process.Process)*tpEngine {
	//TODO:load persisted status
	return &tpEngine{
		engName: eng,
		kv:      kv,
		proc:    proc,
		nextDbNo: 0,//TODO:fix persistence
		nextSerialNo: 0,//TODO:fix persistence
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

//unsafe in multi-thread
func (te *tpEngine) getNextDatabaseNo() uint64 {
	n := te.nextDbNo
	te.nextDbNo++
	return n
}

//unsafe in multi-thread
func (te *tpEngine) getNextSerialNo() uint64 {
	n := te.nextSerialNo
	te.nextSerialNo++
	return n
}

//unsafe in multi-thread
func (te *tpEngine) getDatabaseList() ([]string, error) {
	if te.dbs == nil || len(te.dbs) == 0 {
		key := encodeComparableString(nil,makeEngineDatabasePrefix(te.engName))
		value,err := te.kv.PrefixScan(key,math.MaxInt64)
		if err != nil {
			return nil,err
		}

		for _,v := range value {
			db := bytes2string(v)
			te.dbs = append(te.dbs,db)
			fmt.Printf("%s\n",db)
		}
	}
	return te.dbs,nil
}

//unsafe in multi-thread
func (te *tpEngine) putDatabaseIntoDatabaseList(db string) {
	te.dbs = append(te.dbs,db)
}

//Create database
func (te *tpEngine) Create(db string) error {
	te.rwlock.Lock()
	defer te.rwlock.Unlock()

	//step 1: get database list in the engine
	dbs,err := te.getDatabaseList()
	if err != nil {
		return err
	}

	//step 2: check if the db has existed
	if isInSlice(db,dbs) {
		return fmt.Errorf("database %s has exists")
	}

	//step 3: put the db in the engine with a new id
	nextDbNo := te.getNextDatabaseNo()

	db_skey := makeEngineDatabaseKey(te.engName,nextDbNo)

	db_key := encodeComparableString(nil,db_skey)

	nextSerialNo := te.getNextSerialNo()

	serial_skey := makeEngineSerial(te.engName,nextSerialNo)

	serial_key := encodeComparableString(nil,serial_skey)

	//TODO:add transaction protection
	//db_key -> serial_key
	err = te.kv.Set(db_key,serial_key)
	if err != nil {
		return err
	}

	//serial_key -> dbname
	err = te.kv.Set(serial_key, string2bytes(db))
	if err != nil {
		return err
	}

	te.putDatabaseIntoDatabaseList(db)
	return nil
}

func (te *tpEngine) Delete(db string) error {
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
	//TODO: add transaction protection


	return nil
}