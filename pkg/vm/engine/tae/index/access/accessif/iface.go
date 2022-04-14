package accessif

import "github.com/matrixorigin/matrixone/pkg/container/vector"

type IAppendableBlockIndexHolder interface {
	BatchInsert(keys *vector.Vector, start uint32, count int, offset uint32, verify bool) error
	Delete(key interface{}) error
	Search(key interface{}) (rowOffset uint32, err error)
	BatchDedup(keys *vector.Vector) error
}
