package index

import (
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/compute"
	art "github.com/plar/go-adaptive-radix-tree"
)

type RowsNode struct {
	Ids []uint32
}

func NewRowsNode() *RowsNode {
	return &RowsNode{
		Ids: make([]uint32, 0, 1),
	}
}

func (n *RowsNode) Clone() *RowsNode {
	ids := make([]uint32, len(n.Ids))
	copy(ids, n.Ids)
	return &RowsNode{
		Ids: ids,
	}
}

func (n *RowsNode) Size() int {
	return len(n.Ids)
}

func (n *RowsNode) HasRow(row uint32) (exist bool) {
	for _, id := range n.Ids {
		if id == row {
			exist = true
			break
		}
	}
	return
}

func (n *RowsNode) DeleteRow(row uint32) (pos int, deleted bool) {
	var id uint32
	for pos, id = range n.Ids {
		if id == row {
			deleted = true
			break
		}
	}
	if deleted {
		n.Ids = append(n.Ids[:pos], n.Ids[pos+1:]...)
	}
	return
}

type mvART struct {
	typ  types.Type
	tree art.Tree
}

func NewMultiplRowsART(typ types.Type) *mvART {
	return &mvART{
		typ:  typ,
		tree: art.New(),
	}
}

func (art *mvART) Insert(key any, row uint32) (err error) {
	bufk := compute.EncodeKey(key, art.typ)
	v, found := art.tree.Search(bufk)
	if !found {
		n := NewRowsNode()
		n.Ids = append(n.Ids, row)
		art.tree.Insert(bufk, n)
	} else {
		n := v.(*RowsNode)
		if n.HasRow(row) {
			err = ErrDuplicate
		} else {
			n.Ids = append(n.Ids, row)
		}
	}
	return
}

func (art *mvART) DeleteOne(key any, row uint32) (pos int, err error) {
	bufk := compute.EncodeKey(key, art.typ)
	v, found := art.tree.Search(bufk)
	if !found {
		err = ErrNotFound
	} else {
		n := v.(*RowsNode)
		var deleted bool
		pos, deleted = n.DeleteRow(row)
		if !deleted {
			err = ErrNotFound
		}
	}
	return
}

func (art *mvART) DeleteAll(key any) (err error) {
	bufk := compute.EncodeKey(key, art.typ)
	_, found := art.tree.Delete(bufk)
	if !found {
		err = ErrNotFound
	}
	return
}

func (art *mvART) GetRowsNode(key any) (n *RowsNode, found bool) {
	bufk := compute.EncodeKey(key, art.typ)
	v, found := art.tree.Search(bufk)
	if !found {
		return
	}
	n = v.(*RowsNode)
	// n = v.(*RowsNode).Clone()
	return
}

func (art *mvART) Contains(key any) (found bool) {
	bufk := compute.EncodeKey(key, art.typ)
	_, found = art.tree.Search(bufk)
	return
}

func (art *mvART) ContainsRow(key any, row uint32) (found bool) {
	bufk := compute.EncodeKey(key, art.typ)
	v, found := art.tree.Search(bufk)
	if !found {
		return found
	}
	n := v.(*RowsNode)
	found = n.HasRow(row)
	return
}

func (art *mvART) Size() int {
	return art.tree.Size()
}

func (art *mvART) RowCount(key any) (cnt int) {
	bufk := compute.EncodeKey(key, art.typ)
	v, found := art.tree.Search(bufk)
	if !found {
		cnt = 0
		return
	}
	cnt = v.(*RowsNode).Size()
	return
}
