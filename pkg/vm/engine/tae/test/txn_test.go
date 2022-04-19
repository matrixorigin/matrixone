package test

import (
	"bytes"
	"errors"
	"fmt"
	"math/rand"
	"sync"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	gvec "github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/testutils"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/container/compute"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/handle"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/txnif"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/txn/txnbase"
	"github.com/panjf2000/ants/v2"
	"github.com/stretchr/testify/assert"
)

const (
	ModuleName = "TAETEST"
)

var wareHouse *catalog.Schema
var district *catalog.Schema

var app1db = "app1"
var goods *catalog.Schema
var balance *catalog.Schema
var user *catalog.Schema
var deal *catalog.Schema
var repertory *catalog.Schema
var app1Conf *APP1Conf

var errNotEnoughRepertory = errors.New("not enough repertory")

type APP1Conf struct {
	Users         int
	InitBalance   float64
	GoodKinds     int
	GoodRepertory int
}

type APP1Client struct {
	ID   uint64
	Name string
	Txn  txnif.AsyncTxn
	DB   handle.Database
	Rel  handle.Relation
}

type APP1Goods struct {
	ID    uint64
	Name  string
	Price float64
}

type APP1Repertory struct {
	ID      uint64
	GoodsID uint64
	Count   uint64
}

type APP1 struct {
	sync.RWMutex
	Clients []*APP1Client
	Goods   []*APP1Goods
	DBName  string
	Mgr     *txnbase.TxnManager
}

func init() {
	app1Conf = &APP1Conf{
		Users:         100,
		InitBalance:   1000000,
		GoodKinds:     2000,
		GoodRepertory: 100,
	}

	wareHouse = catalog.NewEmptySchema("WAREHOUSE")
	wareHouse.PrimaryKey = 0
	wareHouse.BlockMaxRows = 40000
	wareHouse.SegmentMaxBlocks = 40
	wareHouse.AppendCol("W_ID", types.Type{Oid: types.T_uint8, Size: 1, Width: 8})
	wareHouse.AppendCol("W_NAME", types.Type{Oid: types.T_varchar, Size: 24, Width: 100})
	wareHouse.AppendCol("W_STREET_1", types.Type{Oid: types.T_varchar, Size: 24, Width: 100})
	wareHouse.AppendCol("W_STREET_2", types.Type{Oid: types.T_varchar, Size: 24, Width: 100})
	wareHouse.AppendCol("W_CITY", types.Type{Oid: types.T_varchar, Size: 24, Width: 100})
	wareHouse.AppendCol("W_STATE", types.Type{Oid: types.T_varchar, Size: 24, Width: 100})
	wareHouse.AppendCol("W_ZIP", types.Type{Oid: types.T_varchar, Size: 24, Width: 100})
	wareHouse.AppendCol("W_TAX", types.Type{Oid: types.T_float64, Size: 8, Width: 64})
	wareHouse.AppendCol("W_YTD", types.Type{Oid: types.T_float64, Size: 8, Width: 64})

	district = catalog.NewEmptySchema("DISTRICT")
	district.PrimaryKey = 0
	district.BlockMaxRows = 40000
	district.SegmentMaxBlocks = 40
	district.AppendCol("D_ID", types.Type{Oid: types.T_int16, Size: 2, Width: 16})
	district.AppendCol("D_W_ID", types.Type{Oid: types.T_uint8, Size: 1, Width: 8})
	district.AppendCol("D_NAME", types.Type{Oid: types.T_varchar, Size: 24, Width: 100})
	district.AppendCol("D_STREET_1", types.Type{Oid: types.T_varchar, Size: 24, Width: 100})
	district.AppendCol("D_STREET_2", types.Type{Oid: types.T_varchar, Size: 24, Width: 100})
	district.AppendCol("D_CITY", types.Type{Oid: types.T_varchar, Size: 24, Width: 100})
	district.AppendCol("D_STATE", types.Type{Oid: types.T_varchar, Size: 24, Width: 100})
	district.AppendCol("D_ZIP", types.Type{Oid: types.T_varchar, Size: 24, Width: 100})
	district.AppendCol("D_TAX", types.Type{Oid: types.T_float64, Size: 8, Width: 64})
	district.AppendCol("D_YTD", types.Type{Oid: types.T_float64, Size: 8, Width: 64})
	district.AppendCol("D_NEXT_O_ID", types.Type{Oid: types.T_int64, Size: 8, Width: 64})

	balance = catalog.NewEmptySchema("BALANCE")
	balance.PrimaryKey = 0
	balance.BlockMaxRows = 40000
	balance.SegmentMaxBlocks = 40
	balance.AppendCol("ID", types.Type{Oid: types.T_uint64, Size: 8, Width: 64})
	balance.AppendCol("BALANCE", types.Type{Oid: types.T_float64, Size: 8, Width: 64})
	// balance.AppendCol("USERID", types.Type{Oid: types.T_uint64, Size: 8, Width: 64})

	user = catalog.NewEmptySchema("USER")
	user.PrimaryKey = 0
	user.BlockMaxRows = 40000
	user.SegmentMaxBlocks = 40
	user.AppendCol("ID", types.Type{Oid: types.T_uint64, Size: 8, Width: 64})
	user.AppendCol("NAME", types.Type{Oid: types.T_varchar, Size: 24, Width: 100})
	user.AppendCol("BIRTH", types.Type{Oid: types.T_date, Size: 4, Width: 32})
	user.AppendCol("ADDR", types.Type{Oid: types.T_varchar, Size: 24, Width: 100})
	user.AppendCol("BALANCEID", types.Type{Oid: types.T_uint64, Size: 8, Width: 64})

	goods = catalog.NewEmptySchema("GOODS")
	goods.PrimaryKey = 0
	goods.BlockMaxRows = 40000
	goods.SegmentMaxBlocks = 40
	goods.AppendCol("ID", types.Type{Oid: types.T_uint64, Size: 8, Width: 64})
	goods.AppendCol("NAME", types.Type{Oid: types.T_varchar, Size: 24, Width: 100})
	goods.AppendCol("PRICE", types.Type{Oid: types.T_float64, Size: 8, Width: 64})
	goods.AppendCol("DESC", types.Type{Oid: types.T_varchar, Size: 24, Width: 100})

	repertory = catalog.NewEmptySchema("REPERTORY")
	repertory.PrimaryKey = 0
	repertory.BlockMaxRows = 40000
	repertory.SegmentMaxBlocks = 40
	repertory.AppendCol("ID", types.Type{Oid: types.T_uint64, Size: 8, Width: 64})
	repertory.AppendCol("GOODID", types.Type{Oid: types.T_uint64, Size: 8, Width: 64})
	repertory.AppendCol("COUNT", types.Type{Oid: types.T_uint64, Size: 8, Width: 64})

	deal = catalog.NewEmptySchema("DEAL")
	deal.PrimaryKey = 0
	deal.BlockMaxRows = 40000
	deal.SegmentMaxBlocks = 40
	deal.AppendCol("ID", types.Type{Oid: types.T_uint64, Size: 8, Width: 64})
	deal.AppendCol("USERID", types.Type{Oid: types.T_uint64, Size: 8, Width: 64})
	deal.AppendCol("GOODID", types.Type{Oid: types.T_uint64, Size: 8, Width: 64})
	deal.AppendCol("QUANTITY", types.Type{Oid: types.T_uint32, Size: 4, Width: 32})
	deal.AppendCol("DEALTIME", types.Type{Oid: types.T_datetime, Size: 8, Width: 64})
}

func NewApp1(mgr *txnbase.TxnManager, dbName string) *APP1 {
	return &APP1{
		Mgr:     mgr,
		DBName:  dbName,
		Clients: make([]*APP1Client, 0),
		Goods:   make([]*APP1Goods, 0),
	}
}

func NewAPP1UserClient(id uint64, name string) *APP1Client {
	return &APP1Client{
		ID:   id,
		Name: name,
	}
}

func (c *APP1Client) Clone() *APP1Client {
	return &APP1Client{
		ID:   c.ID,
		Name: c.Name,
	}
}

func (c *APP1Client) String() string {
	s := fmt.Sprintf("User:%d,%s", c.ID, c.Name)
	return s
}
func (c *APP1Client) Bind(db handle.Database, txn txnif.AsyncTxn) {
	if c.Txn != nil {
		panic("logic error")
	}
	c.Txn = txn
	c.DB = db
	c.Rel, _ = db.GetRelationByName(c.Name)
}

func (c *APP1Client) Unbind() {
	if c.Txn == nil {
		panic("logic error")
	}
	c.Txn = nil
	c.DB = nil
	c.Rel = nil
}

func (c *APP1Client) CheckBound() {
	if c.Txn == nil {
		panic("logic error")
	}
}

// TODO: rewirte
func (c *APP1Client) GetGoodRepetory(goodId uint64) (id *common.ID, offset uint32, count uint64, err error) {
	rel, _ := c.DB.GetRelationByName(repertory.Name)
	blockIt := rel.MakeBlockIt()
	var comp bytes.Buffer
	var decomp bytes.Buffer
	var vec *gvec.Vector
	for blockIt.Valid() {
		comp.Reset()
		decomp.Reset()
		blk := blockIt.GetBlock()
		vec, _, err = blk.GetVectorCopy(repertory.ColDefs[1].Name, &comp, &decomp)
		if err != nil {
			return
		}
		rows := gvec.Length(vec)
		for i := 0; i < rows; i++ {
			v := compute.GetValue(vec, uint32(i))
			if v == goodId {
				id = blk.GetMeta().(*catalog.BlockEntry).AsCommonID()
				offset = uint32(i)
				comp.Reset()
				decomp.Reset()
				vec, _, _ := blk.GetVectorCopy(repertory.ColDefs[2].Name, &comp, &decomp)
				count = compute.GetValue(vec, offset).(uint64)
				return
			}
		}
		blockIt.Next()
	}
	err = catalog.ErrNotFound
	return
}

// TODO: rewrite
func (c *APP1Client) GetGoodEntry(goodId uint64) (id *common.ID, offset uint32, entry *APP1Goods, err error) {
	filter := new(handle.Filter)
	filter.Op = handle.FilterEq
	filter.Val = goodId
	goodRel, _ := c.DB.GetRelationByName(goods.Name)
	id, offset, err = goodRel.GetByFilter(filter)
	if err != nil {
		return
	}

	entry = new(APP1Goods)
	entry.ID = goodId
	price, _ := goodRel.GetValue(id, offset, 2)
	entry.Price = price.(float64)

	// var comp bytes.Buffer
	// var decomp bytes.Buffer
	// for blockIt.Valid() {
	// 	comp.Reset()
	// 	decomp.Reset()
	// 	blk := blockIt.GetBlock()
	// 	vec, err := blk.GetVectorCopy(goods.ColDefs[0].Name, &comp, &decomp)
	// 	if err != nil {
	// 		return id, offset, entry, err
	// 	}
	// 	rows := gvec.Length(vec)
	// 	for i := 0; i < rows; i++ {
	// 		v := compute.GetValue(vec, uint32(i))
	// 		if v == goodId {
	// 			entry = new(APP1Goods)
	// 			entry.ID = goodId
	// 			id = blk.GetMeta().(*catalog.BlockEntry).AsCommonID()
	// 			offset = uint32(i)
	// 			comp.Reset()
	// 			decomp.Reset()
	// 			vec, _ := blk.GetVectorCopy(goods.ColDefs[2].Name, &comp, &decomp)
	// 			entry.Price = compute.GetValue(vec, offset).(float64)
	// 			return id, offset, entry, err
	// 		}
	// 	}
	// 	blockIt.Next()
	// }
	// err = catalog.ErrNotFound
	return
}

func (c *APP1Client) BuyGood(goodId uint64, count uint64) error {
	c.CheckBound()
	_, _, entry, err := c.GetGoodEntry(goodId)
	if err != nil {
		return err
	}
	id, offset, left, err := c.GetGoodRepetory(entry.ID)
	if err != nil {
		return err
	}
	logutil.Debugf("%s, Count=%d", entry.String(), left)
	if count > left {
		logutil.Warnf("NotEnough Good %d: Repe %d, Requested %d", goodId, left, count)
		err = errNotEnoughRepertory
	}
	newLeft := left - count
	rel, _ := c.DB.GetRelationByName(repertory.Name)
	err = rel.Update(id, offset, uint16(2), newLeft)

	return err
}

func (g *APP1Goods) String() string {
	return fmt.Sprintf("GoodId:%d, GoodName:%s, GoodPrice:%f", g.ID, g.Name, g.Price)
}

func MockWarehouses(dbName string, num uint8, txn txnif.AsyncTxn) (err error) {
	db, err := txn.GetDatabase(dbName)
	if err == catalog.ErrNotFound {
		if db, err = txn.CreateDatabase(dbName); err != nil {
			return
		}
	}
	rel, err := db.GetRelationByName(wareHouse.Name)
	if err == catalog.ErrNotFound {
		if rel, err = db.CreateRelation(wareHouse); err != nil {
			return
		}
	}
	bat := compute.MockBatch(wareHouse.Types(), uint64(num), int(wareHouse.PrimaryKey), nil)
	err = rel.Append(bat)
	return
}

func GetWarehouseRelation(dbName string, txn txnif.AsyncTxn) (rel handle.Relation, err error) {
	db, _ := txn.GetDatabase(dbName)
	rel, err = db.GetRelationByName(wareHouse.Name)
	return
}

func GetOrCreateDatabase(name string, txn txnif.AsyncTxn) handle.Database {
	db, err := txn.GetDatabase(name)
	if err == catalog.ErrNotFound {
		if db, err = txn.CreateDatabase(name); err != nil {
			panic(err)
		}
	}
	return db
}

func App1CreateTables(txn txnif.AsyncTxn) (db handle.Database, err error) {
	db = GetOrCreateDatabase(app1db, txn)
	if _, err = db.CreateRelation(user); err != nil {
		return
	}
	if _, err = db.CreateRelation(goods); err != nil {
		return
	}
	if _, err = db.CreateRelation(balance); err != nil {
		return
	}
	if _, err = db.CreateRelation(deal); err != nil {
		return
	}
	if _, err = db.CreateRelation(repertory); err != nil {
		return
	}
	return
}

func (app1 *APP1) GetClient() *APP1Client {
	idx := rand.Intn(len(app1.Clients))
	return app1.Clients[idx].Clone()
}

func (app1 *APP1) GetGoods() *APP1Goods {
	idx := rand.Intn(len(app1.Goods))
	return app1.Goods[idx]
}

func (app1 *APP1) Init(factor int) {
	txn := app1.Mgr.StartTxn(nil)
	defer txn.Commit()
	db, err := App1CreateTables(txn)
	if err != nil {
		panic(err)
	}
	conf := *app1Conf
	conf.GoodKinds *= factor
	conf.GoodRepertory *= factor
	conf.Users *= factor
	balanceRel, err := db.GetRelationByName(balance.Name)
	if err != nil {
		panic(err)
	}
	balanceData := compute.MockBatch(balance.Types(), uint64(conf.Users), int(balance.PrimaryKey), nil)
	if err = balanceRel.Append(balanceData); err != nil {
		panic(err)
	}

	userRel, err := db.GetRelationByName(user.Name)
	if err != nil {
		panic(err)
	}
	provider := compute.NewMockDataProvider()
	provider.AddColumnProvider(4, balanceData.Vecs[0])
	userData := compute.MockBatch(user.Types(), uint64(conf.Users), int(user.PrimaryKey), provider)

	for i := 0; i < conf.Users; i++ {
		uid := compute.GetValue(userData.Vecs[0], uint32(i))
		uname := compute.GetValue(userData.Vecs[1], uint32(i))
		client := NewAPP1UserClient(uid.(uint64), uname.(string))
		app1.Clients = append(app1.Clients, client)
		// logutil.Info(client.String())
	}

	if err = userRel.Append(userData); err != nil {
		panic(err)
	}
	price := gvec.New(goods.ColDefs[2].Type)
	for i := 0; i < conf.GoodKinds; i++ {
		goodPrice := float64(rand.Intn(1000)+20) / float64(rand.Intn(10)+1) / float64(20)
		compute.AppendValue(price, goodPrice)
	}
	goodsRel, err := db.GetRelationByName(goods.Name)
	if err != nil {
		panic(err)
	}
	provider.Reset()
	provider.AddColumnProvider(2, price)
	goodsData := compute.MockBatch(goods.Types(), uint64(conf.GoodKinds), int(goods.PrimaryKey), provider)
	if err = goodsRel.Append(goodsData); err != nil {
		panic(err)
	}

	goodIds := goodsData.Vecs[0]
	count := gvec.New(repertory.ColDefs[2].Type)
	for i := 0; i < conf.GoodKinds; i++ {
		goodCount := rand.Intn(1000) + 100
		compute.AppendValue(count, uint64(goodCount))
		goodsId := compute.GetValue(goodsData.Vecs[0], uint32(i))
		goodsName := compute.GetValue(goodsData.Vecs[1], uint32(i))
		goods := new(APP1Goods)
		goods.ID = goodsId.(uint64)
		goods.Name = goodsName.(string)
		app1.Goods = append(app1.Goods, goods)
	}
	provider.Reset()
	provider.AddColumnProvider(1, goodIds)
	provider.AddColumnProvider(2, count)
	repertoryData := compute.MockBatch(repertory.Types(), uint64(conf.GoodKinds), int(repertory.PrimaryKey), provider)
	repertoryRel, err := db.GetRelationByName(repertory.Name)
	if err != nil {
		panic(err)
	}
	if err = repertoryRel.Append(repertoryData); err != nil {
		panic(err)
	}
}

func TestApp1(t *testing.T) {
	dir := testutils.InitTestEnv(ModuleName, t)
	c, mgr, driver, _, _ := initTestContext(t, dir, common.G*1, common.G)
	defer driver.Close()
	defer c.Close()
	defer mgr.Stop()

	app1 := NewApp1(mgr, "app1")
	app1.Init(1)

	p, _ := ants.NewPool(100)

	var wg sync.WaitGroup
	buyTxn := func() {
		defer wg.Done()
		txn := mgr.StartTxn(nil)
		client := app1.GetClient()
		db, _ := txn.GetDatabase(app1.DBName)
		client.Bind(db, txn)
		goods := app1.GetGoods()
		err := client.BuyGood(goods.ID, uint64(rand.Intn(2)+10))
		if err != nil {
			// t.Log(err)
			txn.Rollback()
		} else {
			txn.Commit()
		}
		if txn.GetTxnState(true) == txnif.TxnStateRollbacked {
			t.Log(txn.String())
		}
	}
	for i := 0; i < 10000; i++ {
		wg.Add(1)
		p.Submit(buyTxn)
	}
	wg.Wait()
	t.Log(c.SimplePPString(common.PPL1))
	{
		txn := mgr.StartTxn(nil)
		db, _ := txn.GetDatabase(app1.DBName)
		rel, _ := db.GetRelationByName(repertory.Name)
		t.Log(rel.SimplePPString(common.PPL1))
	}
}

func TestWarehouse(t *testing.T) {
	dir := testutils.InitTestEnv(ModuleName, t)
	c, mgr, driver, _, _ := initTestContext(t, dir, common.M*1, common.G)
	defer driver.Close()
	defer c.Close()
	defer mgr.Stop()

	txn := mgr.StartTxn(nil)
	err := MockWarehouses("test", 20, txn)
	assert.Nil(t, err)
	assert.Nil(t, txn.Commit())
	t.Log(c.SimplePPString(common.PPL1))

	{
		txn = mgr.StartTxn(nil)
		rel, err := GetWarehouseRelation("test", txn)
		assert.Nil(t, err)
		it := rel.MakeBlockIt()
		blk := it.GetBlock()
		var comp bytes.Buffer
		var decomp bytes.Buffer
		vec, _, _ := blk.GetVectorCopy(wareHouse.ColDefs[1].Name, &comp, &decomp)
		t.Log(vec.String())
	}

}
