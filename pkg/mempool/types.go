package mempool

const (
	CountSize = 8
	PageSize  = 1024
)

type Mempool struct {
	maxSize int
	buckets []bucket
}

type bucket struct {
	size  int
	nslot int
	slots [][]byte
}
