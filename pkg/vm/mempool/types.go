package mempool

const (
	Factor     = 2
	MaxSize    = 1 << 20
	PageOffset = 6
	PageSize   = 64
)

type Mempool struct {
	buckets []bucket
	buffers [][]byte
}

type bucket struct {
	size  int
	slots [][]byte
}
