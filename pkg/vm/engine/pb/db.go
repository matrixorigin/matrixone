package pb

import (
	"matrixone/pkg/vm/engine"

	"github.com/cockroachdb/pebble"
)

func New(name string) (engine.DB, error) {
	if db, err := pebble.Open(name, &pebble.Options{}); err != nil {
		return nil, err
	} else {
		return &pbEngine{db}, nil
	}
}

func (db *pbEngine) Close() error {
	return db.db.Close()
}

func (db *pbEngine) NewBatch() (engine.Batch, error) {
	return &pbBatch{db: db.db, bat: db.db.NewBatch()}, nil
}

func (db *pbEngine) NewIterator(k []byte) (engine.Iterator, error) {
	return &pbIterator{itr: db.db.NewIter(&pebble.IterOptions{
		LowerBound: k,
		UpperBound: upperBound(k),
	})}, nil
}

func (db *pbEngine) Del(k []byte) error {
	return db.db.Delete(k, nil)
}

func (db *pbEngine) Set(k, v []byte) error {
	return db.db.Set(k, v, nil)
}

func (db *pbEngine) Get(k []byte) ([]byte, error) {
	v, c, err := db.db.Get(k)
	if err == pebble.ErrNotFound {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	r := make([]byte, len(v))
	copy(r, v)
	c.Close()
	return r, nil
}

func (b *pbBatch) Cancel() error {
	return b.bat.Close()
}

func (b *pbBatch) Commit() error {
	return b.bat.Commit(nil)
}

func (b *pbBatch) Del(k []byte) error {
	return b.bat.Delete(k, nil)
}

func (b *pbBatch) Set(k, v []byte) error {
	return b.bat.Set(k, v, nil)
}

func (itr *pbIterator) Close() error {
	itr.itr.Close()
	return nil
}

func (itr *pbIterator) Next() error {
	itr.itr.Next()
	return nil
}

func (itr *pbIterator) Valid() bool {
	return itr.itr.Valid()
}

func (itr *pbIterator) Seek(k []byte) error {
	itr.itr.SeekGE(k)
	return nil
}

func (itr *pbIterator) Key() []byte {
	k := itr.itr.Key()
	r := make([]byte, len(k))
	copy(r, k)
	return r
}

func (itr *pbIterator) Value() ([]byte, error) {
	v := itr.itr.Value()
	r := make([]byte, len(v))
	copy(r, v)
	return r, nil
}

func upperBound(k []byte) []byte {
	u := make([]byte, len(k))
	copy(u, k)
	for i := len(u) - 1; i >= 0; i-- {
		u[i] = u[i] + 1
		if u[i] != 0 {
			return u[:i+1]
		}
	}
	return nil
}
