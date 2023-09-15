package util

import (
    "fmt"
    "github.com/matrixorigin/matrixone/pkg/common/moerr"
    logservicepb "github.com/matrixorigin/matrixone/pkg/pb/logservice"
    "reflect"
)

type exporter struct {
    kvs map[string]string
}

func newExporter() *exporter {
    return &exporter{
        kvs: make(map[string]string),
    }
}

func (et *exporter) Export(k, v string) {
    et.kvs[k] = v
}

func (et *exporter) Print() {
    //for k, v := range et.kvs {
    //    //fmt.Println(k, v)
    //}
}

func (et *exporter) Dump() map[string]string {
    return et.kvs
}

func (et *exporter) Clear() {
    et.kvs = make(map[string]string)
}

// flatten is a recursive function to flatten a struct.
// in: the struct object to be flattened. reference or pointer
// name: the name of the struct object. empty if it is the top level struct.
// prefix: the path to the name. empty if it is the top level struct.
// exp: the exporter to export the key-value pairs.
func flatten(in any, name string, prefix string, exp *exporter) error {
    if exp == nil {
        return moerr.NewInternalErrorNoCtx("invalid exporter")
    }
    var err error
    if in == nil {
        //fmt.Printf("%s + %v\n", prefix+name, "nil")
        exp.Export(prefix+name, "nil")
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
        return moerr.NewInternalErrorNoCtx("unsupported type %v", typ.Kind())
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
        //fmt.Printf("%s + %v\n", prefix, value)
        exp.Export(prefix, fmt.Sprintf("%v", value))
    case reflect.Uintptr:
        return moerr.NewInternalErrorNoCtx("unsupported type %v", typ.Kind())
    case reflect.Slice:
        fallthrough
    case reflect.Array:
        if value.Len() == 0 {
            //fmt.Printf("%s%s - %v\n", prefix, typ.Name(), "empty")
            exp.Export(prefix+typ.Name(), "")
        } else {
            for k := 0; k < value.Len(); k++ {
                elemVal := value.Index(k)
                err = flatten(elemVal.Interface(), typ.Name(), oldPrefix+name+"["+fmt.Sprintf("%d", k)+"].", exp)
                if err != nil {
                    return err
                }
            }
        }

    case reflect.Chan:
        return moerr.NewInternalErrorNoCtx("unsupported type %v", typ.Kind())
    case reflect.Func:
        return moerr.NewInternalErrorNoCtx("unsupported type %v", typ.Kind())
    case reflect.Interface:
        return moerr.NewInternalErrorNoCtx("unsupported type %v", typ.Kind())
    case reflect.Map:
        if value.Len() == 0 {
            //fmt.Printf("%s%s - %v\n", prefix, typ.Name(), "empty")
            exp.Export(prefix+typ.Name(), "")
        } else {
            keys := value.MapKeys()
            for _, key := range keys {
                keyVal := value.MapIndex(key)
                err = flatten(keyVal.Interface(), typ.Name(), oldPrefix+name+"<"+fmt.Sprintf("%v", key.Interface())+">.", exp)
                if err != nil {
                    return err
                }
            }
        }

    case reflect.Pointer:
        if value.IsNil() {
            //fmt.Printf("%s%s - %v\n", prefix, typ.Name(), "nil")
            exp.Export(prefix+typ.Name(), "nil")
        } else {
            nextPrefix := ""
            if oldPrefix == "" {
                nextPrefix = oldPrefix
            } else {
                nextPrefix = prefix
            }
            err = flatten(value.Elem().Interface(), typ.Name(), nextPrefix+".", exp)
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
            err = flatten(fieldVal.Interface(), field.Name, prefix+".", exp)
            if err != nil {
                return err
            }
        }
    case reflect.UnsafePointer:
        return moerr.NewInternalErrorNoCtx("unsupported type %v", typ.Kind())
    default:
        return moerr.NewInternalErrorNoCtx("unsupported type %v", typ.Kind())
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
    err := flatten(cfg, "", "", curExp)
    if err != nil {
        return nil, err
    }

    //dump default
    defExp := newExporter()
    err = flatten(defCfg, "", "", defExp)
    if err != nil {
        return nil, err
    }

    //make new map
    for name, value := range curExp.Dump() {
        item := &logservicepb.ConfigItem{
            Name:         name,
            CurrentValue: value,
        }

        //set default value
        if v, ok := defExp.Dump()[name]; ok {
            item.DefaultValue = v
        }

        ret[name] = item
    }
    return ret, err
}
