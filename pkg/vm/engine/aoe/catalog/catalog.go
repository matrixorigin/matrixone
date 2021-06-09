package catalog

import (
	"sync"
)

const (
	mCatalog = "Catalog#1"
)

var (
	gMutex         sync.Mutex
	mCatalogPrefix = []byte("meta1")
	mDBPrefix      = "DB"
	mTablePrefix   = "Table"
	mTableIDPrefix = "TID"
)

func (c *Cat) GetTable(dbID int64, tableID int64) (*TableInfo, error) {
	return &TableInfo{}, nil
	// Check if db exists.
	/*dbKey := m.dbKey(dbID)
	if err := m.checkDBExists(dbKey); err != nil {
		return nil, errors.Trace(err)
	}

	tableKey := m.tableKey(tableID)
	value, err := m.txn.HGet(dbKey, tableKey)
	if err != nil || value == nil {
		return nil, errors.Trace(err)
	}

	tableInfo := &model.TableInfo{}
	err = json.Unmarshal(value, tableInfo)
	return tableInfo, errors.Trace(err)*/
}

func (c *Cat) Get() {

}

//where to generate id

func (c *Cat) checkSchemaExists() {

}

func (c *Cat) checkSchemaBNotExists() {

}

func (c *Cat) checkTableExists() {

}

func (c *Cat) checkTableNotExists() {

}

func (c *Cat) genGlobalUniqIDs(idKey []byte) (uint64, error) {
	return 0, nil
}
