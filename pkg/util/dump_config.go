// Copyright 2022 Matrix Origin
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

package util

import (
	"fmt"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	logservicepb "github.com/matrixorigin/matrixone/pkg/pb/logservice"
	"reflect"
	"strings"
	"sync/atomic"
)

type item struct {
	v       string
	userSet string
}

func (i *item) value() string {
	return i.v
}

func (i *item) internal() string {
	if len(i.userSet) != 0 {
		return i.userSet
	} else {
		return "internal"
	}
}

type exporter struct {
	kvs map[string]item
}

func newExporter() *exporter {
	return &exporter{
		kvs: make(map[string]item),
	}
}

func (et *exporter) Export(k, v, userSet string) {
	k = strings.ToLower(k)
	et.kvs[k] = item{
		v:       v,
		userSet: userSet,
	}
}

func (et *exporter) Print() {
	//for k, v := range et.kvs {
	//	fmt.Println(k, v.v, v.userSet)
	//}
}

func (et *exporter) Dump() map[string]item {
	return et.kvs
}

func (et *exporter) Clear() {
	et.kvs = make(map[string]item)
}

// flatten is a recursive function to flatten a struct.
// in: the struct object to be flattened. reference or pointer
// name: the name of the struct object. empty if it is the top level struct.
// prefix: the path to the name. empty if it is the top level struct.
// userSet: the user_setting tag of the field. empty if it is not set in definition.
// exp: the exporter to export the key-value pairs.
func flatten(in any, name string, prefix string, userSet string, exp *exporter) error {
	if exp == nil {
		return moerr.NewInternalErrorNoCtx("invalid exporter")
	}
	var err error
	if in == nil {
		//fmt.Printf("%s + %v\n", prefix+name, "nil")
		exp.Export(prefix+name, "nil", userSet)
		return nil
	}

	typ := reflect.TypeOf(in)
	value := reflect.ValueOf(in)

	oldPrefix := prefix
	if name != "" {
		prefix = prefix + name
	} else {
		prefix = prefix + typ.Name()
	}

	switch typ.Kind() {
	case reflect.Invalid:
		return moerr.NewInternalErrorNoCtxf("unsupported type %v", typ.Kind())
	case reflect.Bool:
		fallthrough
	case reflect.Int:
		fallthrough
	case reflect.Int8:
		fallthrough
	case reflect.Int16:
		fallthrough
	case reflect.Int32:
		fallthrough
	case reflect.Int64:
		fallthrough
	case reflect.Uint:
		fallthrough
	case reflect.Uint8:
		fallthrough
	case reflect.Uint16:
		fallthrough
	case reflect.Uint32:
		fallthrough
	case reflect.Uint64:
		fallthrough
	case reflect.Float32:
		fallthrough
	case reflect.Float64:
		fallthrough
	case reflect.Complex64:
		fallthrough
	case reflect.Complex128:
		fallthrough
	case reflect.String:
		exp.Export(prefix, fmt.Sprintf("%v", value), userSet)
	case reflect.Uintptr:
		return moerr.NewInternalErrorNoCtxf("unsupported type %v", typ.Kind())
	case reflect.Slice:
		fallthrough
	case reflect.Array:
		if value.Len() == 0 {
			exp.Export(prefix+typ.Name(), "", userSet)
		} else {
			for k := 0; k < value.Len(); k++ {
				elemVal := value.Index(k)
				err = flatten(elemVal.Interface(), typ.Name(), oldPrefix+name+"["+fmt.Sprintf("%d", k)+"].", userSet, exp)
				if err != nil {
					return err
				}
			}
		}

	case reflect.Chan:
		return moerr.NewInternalErrorNoCtxf("unsupported type %v", typ.Kind())
	case reflect.Func:
		exp.Export(prefix+typ.Name(), "", userSet)
	case reflect.Interface:
		exp.Export(prefix+typ.Name(), "", userSet)
	case reflect.Map:
		if value.Len() == 0 {
			exp.Export(prefix+typ.Name(), "", userSet)
		} else {
			keys := value.MapKeys()
			for _, key := range keys {
				keyVal := value.MapIndex(key)
				err = flatten(keyVal.Interface(), typ.Name(), oldPrefix+name+"<"+fmt.Sprintf("%v", key.Interface())+">.", userSet, exp)
				if err != nil {
					return err
				}
			}
		}

	case reflect.Pointer:
		if value.IsNil() {
			exp.Export(prefix+typ.Name(), "nil", userSet)
		} else {
			nextPrefix := ""
			if oldPrefix == "" {
				nextPrefix = oldPrefix
			} else {
				nextPrefix = prefix
			}
			err = flatten(value.Elem().Interface(), typ.Name(), nextPrefix+".", userSet, exp)
			if err != nil {
				return err
			}
		}
	case reflect.Struct:
		for i := 0; i < typ.NumField(); i++ {
			field := typ.Field(i)
			isExported := field.IsExported()
			var fieldVal reflect.Value
			if isExported {
				fieldVal = value.Field(i)
			} else {
				continue
			}

			tagValue := userSet
			if v, ok := field.Tag.Lookup("user_setting"); ok {
				tagValue = v
			}

			err = flatten(fieldVal.Interface(), field.Name, prefix+".", tagValue, exp)
			if err != nil {
				return err
			}
		}
	case reflect.UnsafePointer:
		return moerr.NewInternalErrorNoCtxf("unsupported type %v", typ.Kind())
	default:
		return moerr.NewInternalErrorNoCtxf("unsupported type %v", typ.Kind())
	}
	return err
}

func DumpConfig(cfg any, defCfg any) (map[string]*logservicepb.ConfigItem, error) {
	ret := make(map[string]*logservicepb.ConfigItem)
	if cfg == nil || defCfg == nil {
		return ret, moerr.NewInvalidInputNoCtx("invalid input cfg or defCfg")
	}

	//dump current
	curExp := newExporter()
	err := flatten(cfg, "", "", "", curExp)
	if err != nil {
		return nil, err
	}

	//dump default
	defExp := newExporter()
	err = flatten(defCfg, "", "", "", defExp)
	if err != nil {
		return nil, err
	}

	//make new map
	for name, value := range curExp.Dump() {
		citem := &logservicepb.ConfigItem{
			Name:         name,
			CurrentValue: value.value(),
			Internal:     value.internal(),
		}

		//set default value
		if v, ok := defExp.Dump()[name]; ok {
			citem.DefaultValue = v.value()
		}

		ret[name] = citem
	}
	return ret, err
}

// MergeConfig copy all items from src to dst and overwrite the existed item.
func MergeConfig(dst *ConfigData, src map[string]*logservicepb.ConfigItem) {
	if dst == nil || dst.configData == nil || src == nil {
		return
	}
	for s, item := range src {
		dst.configData[s] = item
	}
}

const (
	count = 50
)

type ConfigData struct {
	count      atomic.Int32
	configData map[string]*logservicepb.ConfigItem
}

func NewConfigData(data map[string]*logservicepb.ConfigItem) *ConfigData {
	ret := &ConfigData{
		count:      atomic.Int32{},
		configData: make(map[string]*logservicepb.ConfigItem, len(data)),
	}
	for k, v := range data {
		ret.configData[k] = v
	}
	ret.count.Store(count)
	return ret
}

func (cd *ConfigData) GetData() *logservicepb.ConfigData {
	if cd.count.Load() > 0 {
		return &logservicepb.ConfigData{
			Content: cd.configData,
		}
	}
	return nil
}

func (cd *ConfigData) DecrCount() {
	if cd.count.Load() > 0 {
		cd.count.Add(-1)
	}
}
