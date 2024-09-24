// Copyright 2023 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package perfcounter

import (
	"fmt"
	"reflect"

	"github.com/matrixorigin/matrixone/pkg/util/metric/stats"
)

type CounterSet struct {
	FileService FileServiceCounterSet
	DistTAE     DistTAECounterSet
	TAE         TAECounterSet
}

type FileServiceCounterSet struct {
	S3 struct {
		List        stats.Counter
		Head        stats.Counter
		Put         stats.Counter
		Get         stats.Counter
		Delete      stats.Counter
		DeleteMulti stats.Counter
	}

	Cache struct {
		Read   stats.Counter
		Hit    stats.Counter
		Memory struct {
			Read stats.Counter
			Hit  stats.Counter
		}
		Disk struct {
			Read             stats.Counter
			Hit              stats.Counter
			GetFileContent   stats.Counter
			SetFileContent   stats.Counter
			OpenIOEntryFile  stats.Counter
			OpenFullFile     stats.Counter
			CreateFile       stats.Counter
			StatFile         stats.Counter
			WriteFile        stats.Counter
			Error            stats.Counter
			Evict            stats.Counter
			EvictPending     stats.Counter
			EvictImmediately stats.Counter
		}
		Remote struct {
			Read stats.Counter
			Hit  stats.Counter
		}
		LRU struct {
			Evict             stats.Counter
			EvictWithZeroRead stats.Counter
		}
	}

	FileWithChecksum struct {
		Read            stats.Counter
		Write           stats.Counter
		UnderlyingRead  stats.Counter
		UnderlyingWrite stats.Counter
	}
}

type DistTAECounterSet struct {
	Logtail struct {
		Entries               stats.Counter
		InsertEntries         stats.Counter
		MetadataInsertEntries stats.Counter
		DeleteEntries         stats.Counter
		MetadataDeleteEntries stats.Counter

		InsertRows   stats.Counter
		DeleteRows   stats.Counter
		ActiveRows   stats.Counter
		InsertBlocks stats.Counter
	}
}

type TAECounterSet struct {
	LogTail struct {
		Entries       stats.Counter
		InsertEntries stats.Counter
		DeleteEntries stats.Counter
	}

	CheckPoint struct {
		DoGlobalCheckPoint      stats.Counter
		DoIncrementalCheckpoint stats.Counter
		DeleteGlobalEntry       stats.Counter
		DeleteIncrementalEntry  stats.Counter
	}

	Object struct {
		Create              stats.Counter
		CreateNonAppendable stats.Counter
		SoftDelete          stats.Counter
		MergeBlocks         stats.Counter
		CompactBlock        stats.Counter
	}

	Block struct {
		Create              stats.Counter
		CreateNonAppendable stats.Counter
		SoftDelete          stats.Counter
		Flush               stats.Counter
	}
}

var statsCounterType = reflect.TypeOf((*stats.Counter)(nil)).Elem()

type IterFieldsFunc func(path []string, counter *stats.Counter) error

func (c *CounterSet) IterFields(fn IterFieldsFunc) error {
	return iterFields(
		reflect.ValueOf(c),
		[]string{},
		fn,
	)
}

func (c *CounterSet) Reset() {
	*c = CounterSet{}
}

func iterFields(v reflect.Value, path []string, fn IterFieldsFunc) error {

	if v.Type() == statsCounterType {
		return fn(path, v.Addr().Interface().(*stats.Counter))
	}

	t := v.Type()

	switch t.Kind() {

	case reflect.Pointer:
		iterFields(v.Elem(), path, fn)

	case reflect.Struct:
		for i := 0; i < t.NumField(); i++ {
			field := t.Field(i)
			if err := iterFields(v.Field(i), append(path, field.Name), fn); err != nil {
				return err
			}
		}

	case reflect.Map:
		if t.Key().Kind() != reflect.String {
			panic(fmt.Sprintf("unknown type: %v", v.Type()))
		}
		iter := v.MapRange()
		for iter.Next() {
			if err := iterFields(iter.Value(), append(path, iter.Key().String()), fn); err != nil {
				return err
			}
		}

	default:
		panic(fmt.Sprintf("unknown type: %v", v.Type()))
	}

	return nil
}
