package dataio

import (
	"context"
	"fmt"
	pkgcatalog "github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/index"
	"strconv"
	"strings"
)

// Reader is the only interface that mo provides for CN/DN/ETL... modules to read data
type Reader interface {
	// LoadColumns loads the data of a block specified column
	// idxs is the column number of the data to be read
	// extent is the address offset information of all block metadata
	// stored in an object, which is obtained after calling Writer.Sync
	// extent.id can find a specific block in an object, extent is not nil
	LoadColumns(ctx context.Context, idxs []uint16, extent objectio.Extent, m *mpool.MPool) (*batch.Batch, error)

	// LoadZoneMaps loads the ZoneMap index of the specified column of the block
	// idxs is the column number of the index to be read
	// extent is not nil
	// Returns an array of ZoneMap data structures
	LoadZoneMaps(ctx context.Context, idxs []uint16, extent objectio.Extent, m *mpool.MPool) ([]*index.ZoneMap, error)

	// LoadAllZoneMaps loads the ZoneMap index of the specified column of all blocks in an object
	// idxs is the column number of the index to be read
	// extent can be nil, if extent=nil, objectio will find metadata through layout structur
	// Returns a two-dimensional array of ZoneMap data structures
	LoadAllZoneMaps(ctx context.Context, idxs []uint16, extent objectio.Extent, m *mpool.MPool) ([][]*index.ZoneMap, error)

	// LoadBloomFilter loads the BloomFilter index of the specified column of the block
	// idx is the column number of the index to be read,Only one column of data in a block has BloomFilter
	// extent is not nil
	// Returns index.StaticFilter data structures
	LoadBloomFilter(ctx context.Context, idx uint16, extent objectio.Extent, m *mpool.MPool) (index.StaticFilter, error)

	// LoadMeta loads the metadata of a block
	// extent is not nil
	// Returns the BlockObject structure of the specified block
	LoadMeta(ctx context.Context, extent objectio.Extent, m *mpool.MPool) (objectio.BlockObject, error)

	// LoadAllMetas loads the metadata of all blocks in an object
	// extent can be nil, if extent=nil, objectio will find metadata through layout structure
	// extent.id is not used
	LoadAllMetas(ctx context.Context, extent objectio.Extent, m *mpool.MPool) ([]objectio.BlockObject, error)

	// LoadAllData loads the data of all blocks in the object
	// extent can be nil, if extent=nil, objectio will find metadata through layout structure
	LoadAllData(ctx context.Context, extent objectio.Extent, m *mpool.MPool) ([]*batch.Batch, error)

	// LoadColumnsByTS loads the data of the column specified by the block at a certain point in time(ts)
	// info is the BlockInfo of the block, including MetaLoc/DeltaLoc/CommitTs... some
	// protocol information, which needs to be processed internally and returned to the caller's visible data
	LoadColumnsByTS(ctx context.Context, idxs []uint16, info pkgcatalog.BlockInfo,
		ts timestamp.Timestamp, m *mpool.MPool) (*batch.Batch, error)
}

// Writer is the only interface that mo provides to CN/DN/ETL... modules to write data
// default no primary key，a buffer will be created in the Writer to store the written batch
type Writer interface {
	// SetPrimaryKey Set the primary key of the writer, because the written
	// data needs to generate BloomFilter according to the primary key
	SetPrimaryKey(idx uint16)

	// WriteBatch writes a batch into the buffer, and at the same time
	// generates a ZoneMap for each column in the batch, and generates
	// a BloomFilter for the primary key if there is a primary key, and
	// these indexes are also written into the buffer
	// Returns metadata of a block (BlockObject)
	// BlockObject.Extent is nil because it is not yet Synced
	WriteBatch(batch *batch.Batch) (objectio.BlockObject, error)

	// WriteBatchWithOutIndex writes a batch into the buffer without generating any index
	WriteBatchWithOutIndex(batch *batch.Batch) (objectio.BlockObject, error)

	// Sync is to write multiple batches written to the buffer to the fileservice at one time
	Sync(ctx context.Context) ([]objectio.BlockObject, error)
}

func DecodeLocation(metaLoc string) (string, objectio.Extent, uint32) {
	info := strings.Split(metaLoc, ":")
	name := info[0]
	location := strings.Split(info[1], "_")
	offset, err := strconv.ParseUint(location[0], 10, 32)
	if err != nil {
		panic(any(err))
	}
	size, err := strconv.ParseUint(location[1], 10, 32)
	if err != nil {
		panic(any(err))
	}
	osize, err := strconv.ParseUint(location[2], 10, 32)
	if err != nil {
		panic(any(err))
	}
	id, err := strconv.ParseUint(location[3], 10, 32)
	if err != nil {
		panic(any(err))
	}
	rows, err := strconv.ParseUint(info[2], 10, 32)
	if err != nil {
		panic(any(err))
	}
	extent := objectio.NewExtent(uint32(id), uint32(offset), uint32(size), uint32(osize))
	return name, extent, uint32(rows)
}

func EncodeLocation(
	name string,
	extent objectio.Extent,
	rows uint32) (string, error) {
	metaLoc := fmt.Sprintf("%s:%d_%d_%d_%d:%d",
		name,
		extent.Offset(),
		extent.Length(),
		extent.OriginSize(),
		extent.Id(),
		rows,
	)
	return metaLoc, nil
}
