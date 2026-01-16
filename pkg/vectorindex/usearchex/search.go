package usearchex

/*
#include <stdlib.h>
#include <string.h>
#include "../../../cgo/bloom.h"
#include "../../../cgo/usearchex.h"
*/
import "C"
import (
	"errors"
	"fmt"
	"unsafe"

	"github.com/matrixorigin/matrixone/pkg/common/bloomfilter"
	usearch "github.com/unum-cloud/usearch/golang"
)

func FilteredSearchUnsafeWithBloomFilter(
	index *usearch.Index,
	query unsafe.Pointer,
	limit uint,
	bf *bloomfilter.CBloomFilter,
) (keys []usearch.Key, distances []float32, err error) {
	var errorMessage *C.char

	if index.GetHandle() == nil {
		panic("index is uninitialized")
	}

	if query == nil {
		return nil, nil, errors.New("query pointer cannot be nil")
	}

	if limit == 0 {
		return []usearch.Key{}, []float32{}, nil
	}

	keys = make([]usearch.Key, limit)
	distances = make([]float32, limit)

	resultCount := uint(C.usearchex_filtered_search_with_bloomfilter(unsafe.Pointer(index.GetHandle()), query, C.uint32_t(index.GetConfig().Quantization.CValue()),
		C.size_t(limit), (unsafe.Pointer)(bf.Ptr()), (*C.usearch_key_t)(&keys[0]), (*C.usearch_distance_t)(&distances[0]), (*C.usearch_error_t)(&errorMessage)))

	if errorMessage != nil {
		return nil, nil, errors.New(C.GoString(errorMessage))
	}
	fmt.Println(resultCount)
	fmt.Println(distances)
	fmt.Println(keys)

	return nil, nil, nil
}
