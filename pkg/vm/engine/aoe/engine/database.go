package engine

import (
	"github.com/fagongzi/util/format"
	log "github.com/sirupsen/logrus"
	"matrixone/pkg/vm/engine"
	"matrixone/pkg/vm/engine/aoe/catalog"
	"matrixone/pkg/vm/engine/aoe/common/helper"
	"matrixone/pkg/vm/metadata"
)

func (db *database) Type() int {
	return db.typ
}

func (db *database) Delete(_ uint64, name string) error {
	_, err := db.catalog.DropTable(db.id, name)
	return err
}

func (db *database) Create(_ uint64, name string, defs []engine.TableDef, pdef *engine.PartitionBy, _ *engine.DistributionBy, comment string) error {
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
	tablets, err := db.catalog.GetTablets(db.id, name)
	if err != nil {
		return nil, err
	}
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
		} else {
			if len(ids.Ids) == 0 {
				continue
			}
			addr := db.catalog.Store.RaftStore().GetRouter().LeaderPeerStore(tbl.ShardId).ClientAddr

			for _, id := range ids.Ids {
				r.segments = append(r.segments, engine.SegmentInfo{
					Version:  ids.Version,
					Id:       string(format.Uint64ToBytes(id)),
					GroupId:  string(format.Uint64ToBytes(tbl.ShardId)),
					TabletId: tbl.Name,
					Node: metadata.Node{
						Id:   addr,
						Addr: addr,
					},
				})
			}

		}
	}
	return r, nil
}
