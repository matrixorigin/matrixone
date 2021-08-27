package logEngine

import (
	"matrixone/pkg/encoding"
	"matrixone/pkg/vm/engine"
	"matrixone/pkg/vm/engine/logEngine/kv"
	"matrixone/pkg/vm/engine/logEngine/meta"
	"matrixone/pkg/vm/metadata"
	"os"
	"path"
)

const (
	MetaKey = "meta"
)

func New(path string) (*logEngine, error) {
	db, err := kv.New(path)
	if err != nil {
		return nil, err
	}
	return &logEngine{db: db, path: path}, nil
}

func (e *logEngine) Create(name string, attrs []metadata.Attribute) error {
	data, err := encoding.Encode(meta.Metadata{Name: name, Attrs: attrs})
	if err != nil {
		return err
	}
	dir := path.Join(e.path, name)
	if _, err := os.Stat(dir); os.IsExist(err) {
		return os.ErrExist
	}
	if err := os.Mkdir(dir, os.FileMode(0775)); err != nil {
		return err
	}
	if err := e.db.Set(path.Join(name, MetaKey), data); err != nil {
		os.RemoveAll(path.Join(e.path, name))
		return err
	}
	return nil
}

func (e *logEngine) Relations() []string {
	return []string{"lineorder_flat"}
}

func (e *logEngine) Relation(name string) (engine.Relation, error) {
	var md meta.Metadata

	data, err := e.db.GetCopy(path.Join(name, MetaKey))
	if err != nil {
		return nil, err
	}
	if err := encoding.Decode(data, &md); err != nil {
		return nil, err
	}
	mp := make(map[string]metadata.Attribute)
	{
		for _, attr := range md.Attrs {
			mp[attr.Name] = attr
		}
	}
	return &relation{
		md: md,
		mp: mp,
		id: name,
		db: e.db,
	}, nil
}
