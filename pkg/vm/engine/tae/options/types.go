package options

import (
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
)

const (
	DefaultTxnCacheSize   = 256 * common.M
	DefaultIndexCacheSize = 128 * common.M
	DefaultMTCacheSize    = 4 * common.G

	DefaultBlockMaxRows     = uint32(40000)
	DefaultBlocksPerSegment = uint16(40)
)

type Options struct {
	CacheCfg   *CacheCfg   `toml:"cache-cfg"`
	StorageCfg *StorageCfg `toml:"storage-cfg"`
	Catalog    *catalog.Catalog
}
