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
			Read      stats.Counter
			Hit       stats.Counter
			Capacity  stats.Counter
			Used      stats.Counter
			Available stats.Counter
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
	c.FileService.Reset()
	c.DistTAE.Reset()
	c.TAE.Reset()
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

func (f *FileServiceCounterSet) Reset() {
	f.S3.List.Reset()
	f.S3.Head.Reset()
	f.S3.Put.Reset()
	f.S3.Get.Reset()
	f.S3.Delete.Reset()
	f.S3.DeleteMulti.Reset()

	f.Cache.Read.Reset()
	f.Cache.Hit.Reset()

	f.Cache.Memory.Read.Reset()
	f.Cache.Memory.Hit.Reset()
	f.Cache.Memory.Capacity.Reset()
	f.Cache.Memory.Used.Reset()
	f.Cache.Memory.Available.Reset()

	f.Cache.Disk.Read.Reset()
	f.Cache.Disk.Hit.Reset()
	f.Cache.Disk.GetFileContent.Reset()
	f.Cache.Disk.SetFileContent.Reset()
	f.Cache.Disk.OpenIOEntryFile.Reset()
	f.Cache.Disk.OpenFullFile.Reset()
	f.Cache.Disk.CreateFile.Reset()
	f.Cache.Disk.StatFile.Reset()
	f.Cache.Disk.WriteFile.Reset()
	f.Cache.Disk.Error.Reset()
	f.Cache.Disk.Evict.Reset()
	f.Cache.Disk.EvictPending.Reset()
	f.Cache.Disk.EvictImmediately.Reset()

	f.Cache.Remote.Read.Reset()
	f.Cache.Remote.Hit.Reset()

	f.Cache.LRU.Evict.Reset()
	f.Cache.LRU.EvictWithZeroRead.Reset()

	f.FileWithChecksum.Read.Reset()
	f.FileWithChecksum.Write.Reset()
	f.FileWithChecksum.UnderlyingRead.Reset()
	f.FileWithChecksum.UnderlyingWrite.Reset()
}

func (d *DistTAECounterSet) Reset() {
	d.Logtail.Entries.Reset()
	d.Logtail.InsertEntries.Reset()
	d.Logtail.MetadataInsertEntries.Reset()
	d.Logtail.DeleteEntries.Reset()
	d.Logtail.MetadataDeleteEntries.Reset()

	d.Logtail.InsertRows.Reset()
	d.Logtail.DeleteRows.Reset()
	d.Logtail.ActiveRows.Reset()
	d.Logtail.InsertBlocks.Reset()
}

func (t *TAECounterSet) Reset() {
	t.LogTail.Entries.Reset()
	t.LogTail.InsertEntries.Reset()
	t.LogTail.DeleteEntries.Reset()

	t.CheckPoint.DoGlobalCheckPoint.Reset()
	t.CheckPoint.DoIncrementalCheckpoint.Reset()
	t.CheckPoint.DeleteGlobalEntry.Reset()
	t.CheckPoint.DeleteIncrementalEntry.Reset()

	t.Object.Create.Reset()
	t.Object.CreateNonAppendable.Reset()
	t.Object.SoftDelete.Reset()
	t.Object.MergeBlocks.Reset()
	t.Object.CompactBlock.Reset()

	t.Block.Create.Reset()
	t.Block.CreateNonAppendable.Reset()
	t.Block.SoftDelete.Reset()
	t.Block.Flush.Reset()
}
