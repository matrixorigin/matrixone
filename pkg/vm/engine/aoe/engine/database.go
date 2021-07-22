package engine

import (
	"github.com/fagongzi/util/format"
	log "github.com/sirupsen/logrus"
	stdLog "log"
	"matrixone/pkg/vm/engine"
	"matrixone/pkg/vm/engine/aoe/catalog"
	"matrixone/pkg/vm/engine/aoe/common/helper"
	"matrixone/pkg/vm/metadata"
	"time"
)



func (db *database) Type() int {
	return db.typ
}

func (db *database) Delete(name string) error {
	stdLog.Printf("[QQQQQQ]Call Delete Table, %s", name)
	_, err :=  db.catalog.DropTable(db.id, name)
	return err
}

func (db *database) Create(name string, defs []engine.TableDef, pdef *engine.PartitionBy, _ *engine.DistributionBy, comment string) error {
	stdLog.Printf("[QQQQQQ]Call Create Table, %s", name)
	tbl, err := helper.Transfer(db.id, 0, 0, name, comment, defs, pdef)
	if err != nil {
		return err
	}
	_, err = db.catalog.CreateTable(db.id, tbl)
	return err
}

func (db *database) Relations() []string {
	var rs []string
	tbs, err := db.catalog.GetTables(db.id)
	if err != nil {
		return rs
	}
	for _, tb := range tbs {
		rs = append(rs, tb.Name)
	}
	return rs
}

func (db *database) Relation(name string) (engine.Relation, error) {
	t0 := time.Now()
	tablets, err := db.catalog.GetTablets(db.id, name)
	if err != nil {
		return nil, err
	}
	stdLog.Printf("[QQQQQQ]Call database.Relation, GetTablets %s finished, cost %d ms", name, time.Since(t0).Milliseconds())
	if tablets == nil || len(tablets) == 0 {
		return nil, catalog.ErrTableNotExists
	}

	r := &relation{
		pid:     db.id,
		tbl:     &tablets[0].Table,
		catalog: db.catalog,
	}
	r.tablets = tablets
	for _, tbl := range tablets {
		if ids, err := db.catalog.Store.GetSegmentIds(tbl.Name, tbl.ShardId); err != nil {
			log.Errorf("get segmentInfos for tablet %s failed, %s", tbl.Name, err.Error())
		}else {
			if len(ids.Ids)==0{
				continue
			}
			addr := db.catalog.Store.RaftStore().GetRouter().LeaderAddress(tbl.ShardId)

			for _, id := range ids.Ids {
				r.segments = append(r.segments, engine.SegmentInfo{
					Version:  ids.Version,
					Id:       string(format.Uint64ToBytes(id)),
					GroupId:  string(format.Uint64ToBytes(tbl.ShardId)),
					TabletId: tbl.Name,
					Node: metadata.Node{
						Id: addr,
						Addr: addr,
					},
				})
			}

		}
	}
	stdLog.Printf("[QQQQQQ]Call database.Relation, %s finished, cost %d ms", name, time.Since(t0).Milliseconds())
	return r, nil
}

