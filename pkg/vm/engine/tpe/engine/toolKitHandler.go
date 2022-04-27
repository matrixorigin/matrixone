package engine

import (
	"bufio"
	"errors"
	"fmt"
	"math"
	"os"
	"strconv"
	"strings"

	"github.com/matrixorigin/matrixone/pkg/container/batch"
	vm_engine "github.com/matrixorigin/matrixone/pkg/vm/engine"
)

func DumpDatabaseInfo(eng vm_engine.Engine, args []string) error {
    if len(args) < 2 {
        return nil
    }
    opt := &batch.DumpOption{}
    err := getParamFromCommand(opt, args)
    if err != nil {
        return err
    }

    if opt.Filename != "" {
        file, err := os.OpenFile(opt.Filename, os.O_RDWR | os.O_CREATE, 0o666)
        if err != nil {
            return err
        }
        defer file.Close()
        opt.Writer = bufio.NewWriter(file)
    }
    _, err = DumpTableInfo(eng, opt)
    if err != nil {
        return err
    }
    return nil
}

func getParamFromCommand(opt *batch.DumpOption, args []string) error {
    for i := 1; i < len(args); i++ {
        str := strings.ToLower(args[i])
        if str == "-db" {
            if i + 1 >= len(args) {
                return errors.New("the dbname do not input")
            }
            opt.Db_name = []string{args[i + 1]}
            i++
        } else if str == "-table" {
            if i + 1 >= len(args) {
                return errors.New("the table do not input")
            }
            opt.Table_name = []string{args[i + 1]}
            i++
        } else if str == "-keys" {
            opt.Keys = true
        } else if str == "-values" {
            opt.Values = true
        } else if str == "-decode_key" {
            opt.Decode_key = true
        } else if str == "-decode_value" {
            opt.Decode_value = true
        } else if str == "-limit" {
            if i + 1 >= len(args) {
                return errors.New("the limit param do not input")
            }
            limit := strings.Split(args[i + 1], ",")
            if len(limit) != 2 {
                return errors.New("the input limit param format is not correcr")
            }
            for _, l := range limit {
                n, err := strconv.ParseUint(l, 10, 64)
                if err != nil {
                    return err
                }
                opt.Limit = append(opt.Limit, n)
            }
            i++
        } else if str == "-export" {
            if i + 1 >= len(args) {
                return errors.New("the export filename do not input")
            }
            opt.Filename = args[i + 1]
            i++
        } else if str == "-getvalueofkey" {
            if i + 1 >= len(args) {
                return errors.New("the getValueofKey param do not input")
            }
            keys := strings.Split(args[i + 1], ",")
            for _, k := range keys {
                n, err := strconv.ParseInt(k, 10, 64)
                if err != nil {
                    return err
                }
                if n < 0 || n > 255 {
                    return errors.New("the value is out of range of byte")
                }
                opt.PrimaryKey = append(opt.PrimaryKey, byte(n))
            }
            opt.UseKey = true
            opt.ReadCnt = math.MaxUint64
            i++
        } else if str == "-getvalueoforiginkey" {
            opt.UseValue = true
            if i + 1 >= len(args) {
                return errors.New("the getValueofOriginKey param do not input")
            }
            opt.PrimaryValue = strings.Split(args[i + 1], ",")
            opt.ReadCnt = math.MaxUint64
            i++
        } else {
            return errors.New("Unmatch keyword, please check the input")
        }
    }
    return nil
}

var DumpTableInfo = func(eng vm_engine.Engine, opt *batch.DumpOption) (*batch.DumpResult, error) {
    var result *batch.DumpResult
    tpe_eng, ok := eng.(*TpeEngine)
    if !ok {
        return nil, errors.New("TpeEngine convert failed")
    }
    if opt.Db_name == nil {
        opt.Db_name = tpe_eng.Databases()
    }
    for _, db_name := range opt.Db_name {
        if db_name == "system" {
            continue
        }
        db, err := eng.Database(db_name)
        if err != nil {
            return nil, err
        }
        if opt.Table_name == nil {
            opt.Table_name = db.Relations()
        }

        for _, table_name := range opt.Table_name {
            table, err := db.Relation(table_name)
            if err != nil {
                return nil, err
            }
            tpe_relation, ok := table.(*TpeRelation)
            if !ok {
                return nil, errors.New("TpeRelation convert failed")
            }
            var reader *TpeReader = GetTpeReaderInfo(tpe_relation, tpe_eng, opt)

            refCnts, attrs := MakeReadParam(tpe_relation)

            result, err = getTableData(reader, refCnts, attrs, opt)
            if err != nil {
                return nil, err
            }
        }
    }
    return result, nil
}

func getTableData(tr *TpeReader, refCnts []uint64, attrs []string, opt* batch.DumpOption) (*batch.DumpResult, error) {
    bat, err := tr.Read(refCnts, attrs)
    if err != nil {
        return nil, err
    }
    result := bat.Result
    if result == nil {
        return nil, nil
    }

    if opt.Limit != nil && len(opt.Limit) == 2 {
        if opt.Limit[0] + opt.Limit[1] > uint64(len(result.Keys)) {
            return nil, errors.New("The limit range is out of the result")
        }
        // truncate the result vec
        result.Keys = result.Keys[opt.Limit[0]:opt.Limit[0] + opt.Limit[1]]
        result.Values = result.Values[opt.Limit[0]:opt.Limit[0] + opt.Limit[1]]
        for i := 0; i < len(result.Decode_keys.Vecs); i++ {
            result.Decode_keys.Vecs[i] = result.Decode_keys.Vecs[i][opt.Limit[0]:opt.Limit[0] + opt.Limit[1]]
        }
        for i := 0; i < len(result.Decode_values.Vecs); i++ {
            result.Decode_values.Vecs[i] = result.Decode_values.Vecs[i][opt.Limit[0]:opt.Limit[0] + opt.Limit[1]]
        }
    }

    if opt.Filename == "" {
        dumpTableDataToshell(opt, result)
    } else {
        dumpTableDataToFile(opt, result)
    }
    return result, nil
}

func getDumpDataHeader(opt* batch.DumpOption, result *batch.DumpResult) string {
    var header string
    if opt.Keys {
        header += "Keys"
    }
    if opt.Values {
        if header != "" {
            header += "\t\t\t\t"
        }
        header += "Values"
    }
    if opt.Decode_key {
        if header != "" {
            header += "\t\t\t\t"
        }
        header += fmt.Sprintf("%v", result.Decode_keys.Attrs)
    }
    if opt.Decode_value {
        if header != "" {
            header += "\t\t\t\t"
        }
        header += fmt.Sprintf("%v", result.Decode_values.Attrs)
    }
    return header
}

func getDumpDataBody(opt* batch.DumpOption, result *batch.DumpResult, i int) string {
    var body string
    if opt.Keys {
        body += fmt.Sprintf("%v", result.Keys[i])
    }
    if opt.Values {
        if body != "" {
            body += "\t"
        }
        body += fmt.Sprintf("%v", result.Values[i])
    }

    if opt.Decode_key {
        if body != "" {
            body += "\t"
        }
        tmp := ""
        for j, _ := range result.Decode_keys.Attrs {
            tmp += fmt.Sprintf("%v", result.Decode_keys.Vecs[j][i])
            if j != len(result.Decode_keys.Attrs) - 1 {
                tmp += ","
            }
        }
        body += tmp
    }

    if opt.Decode_value {
        if body != "" {
            body += "\t"
        }
        tmp := ""
        for j, _ := range result.Decode_values.Attrs {
            tmp += fmt.Sprintf("%v", result.Decode_values.Vecs[j][i])
            if j != len(result.Decode_values.Attrs) - 1 {
                tmp += ","
            }
        }
        body += tmp
    }
    return body
}

func dumpTableDataToshell(opt* batch.DumpOption, result *batch.DumpResult) error {
    fmt.Println()
    header := getDumpDataHeader(opt, result)
    fmt.Println(header)

    for i := 0; i < len(result.Keys); i++ {
        body := getDumpDataBody(opt, result, i)
        fmt.Println(body)
    }
    return nil
}

func dumpTableDataToFile(opt* batch.DumpOption, result *batch.DumpResult) error {
    header := getDumpDataHeader(opt, result)
    _, err := opt.Writer.WriteString(header + "\n")
    if err != nil {
        return err
    }

    for i := 0; i < len(result.Keys); i++ {
        body := getDumpDataBody(opt, result, i) + "\n"
        _, err = opt.Writer.WriteString(body)
        if err != nil {
            return err
        }
    }
    opt.Writer.Flush()
    return nil
} 