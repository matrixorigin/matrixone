// Copyright 2021 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package tpEngine

import (
	"bytes"
	"fmt"
	"math"
	"github.com/matrixorigin/matrixone/pkg/encoding"
	"github.com/matrixorigin/matrixone/pkg/vm/driver"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

func NewTpDatabase(n string, id uint64, cinfo string, sch string, t int, kv driver.CubeDriver, proc *process.Process) *tpDatabase {
	return &tpDatabase{
		dbName:      n,
		dbId:        id,
		createInfo:  cinfo,
		schema:      sch,
		engineType:  t,
		nextDbNo:    0, //TODO:load from meta1
		nextTableNo: 0, //TODO:load from meta1
		proc:        proc,
		kv:          kv,
		rels:        make(map[string]*tpTupleImpl),
	}
}

func (td *tpDatabase) hasRelation(name string) bool {
	_, ok := td.rels[name]
	return ok
}

func (td *tpDatabase) saveRelation(name string, meta *tpTupleImpl) {
	td.rels[name] = meta
}

func (td *tpDatabase) removeRelation(name string) {
	_, ok := td.rels[name]
	if !ok {
		return
	}

	delete(td.rels, name)
	//TODO:
	return
}

//unsafe in multi-threaded
func (td *tpDatabase) loadRelationList() error {
	prefix := NewTpTableKey(tpEngineName,
		td.dbId,
		TABLE_TABLES_ID,
		uint64(0),
		nil,
		nil,
		nil,
		nil,
	)
	prefix_key := prefix.encodePrefix(nil)
	value, err := td.kv.PrefixScan(prefix_key, math.MaxUint64)
	if err != nil {
		return err
	}

	for i := 0; i < len(value); i += 2 {
		k := value[i]
		v := value[i+1]
		if bytes.HasPrefix(k, prefix_key) {
			_, keys, err := decodeKeys(k[len(prefix_key):], TABLE_TABLES_PRIMARY_KEY_SCHEMA)
			if err != nil {
				return err
			}
			db := keys[0].(string)
			fmt.Printf("+++> rel: %v \n", db)
			if !td.hasRelation(db) {
				dbRow := &tpTupleImpl{}
				_, err = dbRow.decode(v)
				if err != nil {
					return err
				}
				td.saveRelation(db, dbRow)
			}
		}

		//fmt.Printf("+++> %s\n",string(v))
	}

	return nil
}

func (td *tpDatabase) Type() int {
	//TODO:update
	return engine.RSE
}

func (td *tpDatabase) Relations() []string {
	td.rwlock.Lock()
	defer td.rwlock.Unlock()
	err := td.loadRelationList()
	if err != nil {
		panic(err)
	}

	keys := make([]string, 0, len(td.rels))
	for k := range td.rels {
		keys = append(keys, k)
	}

	return keys
}

func (td *tpDatabase) Relation(name string) (engine.Relation, error) {
	td.rwlock.Lock()
	defer td.rwlock.Unlock()

	err := td.loadRelationList()
	if err != nil {
		return nil, err
	}

	if !td.hasRelation(name) {
		return nil, fmt.Errorf("relation %v does not exist", name)
	}

	relRow := td.rels[name]
	relId := relRow.fields[0].(uint64)      //rel id
	createInfo := relRow.fields[1].(string) //create info
	sch := relRow.fields[2].([]byte)        //sch
	var md tpMetadata

	//TODO: if it is the system table, the schema decode should be different.
	err = encoding.Decode(sch, &md)
	if err != nil {
		return nil, err
	}

	return NewTpRelation(name, relId, createInfo, md, td.kv, td.proc), nil
}

func (td *tpDatabase) Delete(_ uint64, rel string) error {
	td.rwlock.Lock()
	defer td.rwlock.Unlock()

	//step 1: get relation list
	err := td.loadRelationList()
	if err != nil {
		return err
	}

	//step 2: check if the relation has existed
	if !td.hasRelation(rel) {
		return nil
	}

	//step 3: remove the relation from the database
	rel_skey := NewTpTableKey(tpEngineName,
		td.dbId,
		TABLE_TABLES_ID,
		0,
		TABLE_TABLES_PRIMARY_KEY_SCHEMA,
		[]interface{}{rel},
		nil,
		nil)

	rel_key := rel_skey.encode(nil)
	err = td.kv.Delete(rel_key)
	if err != nil {
		return err
	}

	return nil
}

//load table meta1 of the database
func (td *tpDatabase) loadMeta1() error {
	meta_skey := NewTpTableKey(tpEngineName,
		td.dbId, TABLE_META1_ID, 0,
		TABLE_META1_PRIMARY_KEY_SCHEMA,
		[]interface{}{TABLE_META1_PRIMARY_KEY},
		nil,
		nil)
	meta_key := meta_skey.encode(nil)
	value, err := td.kv.Get(meta_key)
	if err != nil {
		return err
	}

	if value == nil || len(value) == 0 {
		return fmt.Errorf("meta1 does not have pk")
	}

	row := &tpTupleImpl{}
	_, err = row.decode(value)
	if err != nil {
		return err
	}

	td.nextDbNo = row.fields[0].(uint64)
	td.nextTableNo = row.fields[1].(uint64)

	return nil
}

//store table meta1 of the database 0
func (td *tpDatabase) storeMeta1() error {
	meta_skey := NewTpTableKey(tpEngineName,
		td.dbId, TABLE_META1_ID, 0,
		TABLE_META1_PRIMARY_KEY_SCHEMA,
		[]interface{}{TABLE_META1_PRIMARY_KEY},
		nil,
		nil)
	meta_key := meta_skey.encode(nil)
	metaRow := NewTpTupleImpl(TABLE_META1_REST_SCHEMA,
		uint64(td.nextDbNo), uint64(td.nextTableNo))
	meta_val := metaRow.encode(nil)
	err := td.kv.Set(meta_key, meta_val)
	if err != nil {
		return err
	}

	return nil
}

func (td *tpDatabase) getNextTableNo() uint64 {
	n := td.nextTableNo
	td.nextTableNo++
	return n
}

func (td *tpDatabase) Create(_ uint64, rel string, defs []engine.TableDef, _ *engine.PartitionBy, _ *engine.DistributionBy, _ string) error {
	td.rwlock.Lock()
	defer td.rwlock.Unlock()

	//step 1: get relation list
	err := td.loadRelationList()
	if err != nil {
		return err
	}

	//step 2: check if the relation has existed
	if td.hasRelation(rel) {
		return fmt.Errorf("relation %s has existed", rel)
	}

	err = td.loadMeta1()
	if err != nil {
		return err
	}

	tabNo := td.getNextTableNo()

	//step 3: add the relation into the database
	rel_skey := NewTpTableKey(tpEngineName,
		td.dbId,
		TABLE_TABLES_ID,
		0,
		TABLE_TABLES_PRIMARY_KEY_SCHEMA,
		[]interface{}{rel},
		nil,
		nil)

	rel_key := rel_skey.encode(nil)

	//encode the metadata into bytes
	var md tpMetadata
	var attrs []engine.Attribute

	for _, def := range defs {
		v, ok := def.(*engine.AttributeDef)
		if ok {
			attrs = append(attrs, v.Attr)
		}
	}

	md.Name = rel
	md.Attrs = attrs
	data, err := encoding.Encode(md)
	if err != nil {
		return err
	}

	relRow := NewTpTupleImpl(TABLE_TABLES_REST_SCHEMA,
		tabNo, "create table info", data,
	)

	relValue := relRow.encode(nil)
	err = td.kv.Set(rel_key, relValue)
	if err != nil {
		return err
	}

	err = td.storeMeta1()
	if err != nil {
		return err
	}

	err = td.loadRelationList()
	if err != nil {
		return err
	}

	return nil
}
