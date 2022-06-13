package model

// type Buckets struct {
// 	Type types.Type
// 	Map  map[common.ID]*vector.Vector
// }

// func NewBuckets(typ types.Type) *Buckets {
// 	return &Buckets{
// 		Type: typ,
// 		Map:  make(map[common.ID]*vector.Vector),
// 	}
// }

// func (bs *Buckets) Insert(id *common.ID, v any) (err error) {
// 	bucket := bs.Map[*id]
// 	if bucket == nil {
// 		bucket = vector.New(bs.Type)
// 		bs.Map[*id] = bucket
// 	}
// 	compute.AppendValue(bucket, v)
// 	return
// }

// func (bs *Buckets) ForEachBucket(op func(id *common.ID, bucket *vector.Vector) error) (err error) {
// 	for bid, b := range bs.Map {
// 		if err = op(&bid, b); err != nil {
// 			break
// 		}
// 	}
// 	return
// }
