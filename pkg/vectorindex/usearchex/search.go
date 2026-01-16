package usearchex

/*
#include <stdlib.h>
#include <string.h>
#include "../../../cgo/usearchex.h"
*/
import "C"
import (
	"unsafe"

	"github.com/matrixorigin/matrixone/pkg/common/bloomfilter"
	usearch "github.com/unum-cloud/usearch/golang"
)

func FilteredSearchUnsafeWithBloomFilter(
	index *usearch.Index,
	query unsafe.Pointer,
	limit uint,
	bf *bloomfilter.CBloomFilter,
) (keys []Key, distances []float32, err error) {

	resultCount := uint(C.usearchex_filtered_search_with_bloomfilter(index.GetHandle(), query, index.GetConfig().Quantization.CValue(),
		bf.Ptr(), (*C.usearch_key_t)(&keys[0]), (*C.usearch_distance_t)(&distances[0]), (*C.usearch_error_t)(&errorMessage)))

	return nil, nil, nil
}
