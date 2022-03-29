package tpe

import (
	"bufio"
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"

	"github.com/matrixorigin/matrixone/pkg/container/batch"
	vm_engine "github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tpe/engine"
)

// 从指定Key开始导出存储中指定个数的key
func DumpKeys(startKey []byte, count uint64, options []batch.DumpOption) (keys [][]byte,values [][]byte, err error) {
	return nil, nil, nil
}

// 取keys的values值
func GetKeys(keys [][]byte,options []batch.DumpOption) ([][]byte,error) {
    return nil, nil
}

// 将不可读的keys按格式解码为可读串
func DecodeKeys(keys [][]byte,options []batch.DumpOption) ([][]byte, error) {
    return nil, nil
}

// 将不可读的values按格式解码为可读串
func DecodeValues(keys [][]byte,options []batch.DumpOption) ([][]byte, error) {
    return nil, nil
}

// 导出存储中指定租客，表，索引的指定个数的keyValue
func DumpTable(tenant,table,index string,count uint64,options []batch.DumpOption)(keys [][]byte,values [][]byte, err error) {
    return nil, nil, nil
}

// 取描述符
func GetDescByID(id uint64)([]byte,error) {
    return nil, nil
}

// 取表描述符
func GetTableDescByName(database, name string)([]byte,error) {
    return nil, nil
}

// 取数据库描述符
func GetDatabaseDescByName(database string)([]byte,error) {
    return nil, nil
}

// 追踪信息。记录信息到包括不限于日志，数据表，会话中
func Trace(format string,args ...interface{}) {
}

func DumpDatabaseInfo(eng vm_engine.Engine, args []string) error {
    if len(args) < 2 {
        return nil
    }
    var opt batch.DumpOption
    err := getParamFromCommand(&opt, args)
    if err != nil {
        return err
    }

    if opt.Filename != "" {
        file, err := os.OpenFile(opt.Filename, os.O_RDWR | os.O_EXCL | os.O_CREATE, 0o666)
        if err != nil {
            return err
        }
        defer file.Close()
        opt.Writer = bufio.NewWriter(file)
    }
    err = DumpTableInfo(eng, &opt)
    if err != nil {
        return err
    }
    return nil
}

func getParamFromCommand(opt *batch.DumpOption, args []string) error {
    for i := 1; i < len(args); i++ {
        str := strings.ToLower(args[i])
        if str == "db" {
            if i + 1 >= len(args) {
                return errors.New("the dbname do not input")
            }
            opt.Db_name = []string{args[i + 1]}
            i++
        } else if str == "table" {
            if i + 1 >= len(args) {
                return errors.New("the table do not input")
            }
            opt.Table_name = []string{args[i + 1]}
            i++
        } else if str == "keys" {
            opt.Keys = true
        } else if str == "values" {
            opt.Values = true
        } else if str == "decode_key" {
            opt.Decode_key = true
        } else if str == "decode_value" {
            opt.Decode_value = true
        } else if str == "limit" {
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
        } else if str == "export" {
            if i + 1 >= len(args) {
                return errors.New("the export filename do not input")
            }
            opt.Filename = args[i + 1]
            i++
        } else {
            return errors.New("Unmatch keyword, please check the input")
        }
    }
    return nil
}

func DumpTableInfo(eng vm_engine.Engine, opt *batch.DumpOption) error {
    tpe_eng, ok := eng.(*engine.TpeEngine)
    if !ok {
        return errors.New("TpeEngine convert failed")
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
            return err
        }
        if opt.Table_name == nil {
            opt.Table_name = db.Relations()
        }

        for _, table_name := range opt.Table_name {
            table, err := db.Relation(table_name)
            if err != nil {
                return err
            }
            tpe_relation, ok := table.(*engine.TpeRelation)
            if !ok {
                return errors.New("TpeRelation convert failed")
            }
            engine.PrintTpeRelation(tpe_relation)

            var reader *engine.TpeReader = engine.GetTpeReaderInfo(tpe_relation, tpe_eng)

            refCnts, attrs := engine.MakeReadParam(tpe_relation)

            err = getTableData(reader, refCnts, attrs, opt)
            if err != nil {
                return err
            }
        }
    }
    return nil
}

func getTableData(tr *engine.TpeReader, refCnts []uint64, attrs []string, opt* batch.DumpOption) error {
    result, _ := tr.DumpRead(refCnts, attrs, opt)

    if opt.Limit != nil && len(opt.Limit) == 2 {
        if opt.Limit[0] + opt.Limit[1] > uint64(len(result.Keys)) {
            return errors.New("The limit range is out of the result")
        }
        // truncate the result vec
        result.Keys = result.Keys[opt.Limit[0]:opt.Limit[0] + opt.Limit[1]]
        result.Values = result.Values[opt.Limit[0]:opt.Limit[0] + opt.Limit[1]]
        // result.Decode_keys.Vecs = result.Decode_keys.Vecs[opt.Limit[0]:opt.Limit[0] + opt.Limit[1]]
        // result.Decode_values.Vecs = result.Decode_values.Vecs[opt.Limit[0]:opt.Limit[0] + opt.Limit[1]]
    }

    if opt.Filename == "" {
        dumpTableDataToshell(opt, result)
    } else {
        dumpTableDataToFile(opt, result)
    }
    return nil
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
        header += "["
        for i, attr := range result.Decode_keys.Attrs {
            header += attr
            if i != len(result.Decode_keys.Attrs) - 1 {
                header += ","
            }
        }
        header += "]"
    }
    if opt.Decode_value {
        if header != "" {
            header += "\t\t\t\t"
        }
        header += "["
        for i, attr := range result.Decode_values.Attrs {
            header += attr
            if i != len(result.Decode_values.Attrs) - 1 {
                header += ","
            }
        }
        header += "]"
    }
    return header
}

func outputOneLine(opt* batch.DumpOption, str interface{}) error {
    switch str.(type) {
    case []byte, batch.DumpKey, batch.DumpValue:
        if opt.Filename == "" {
            fmt.Print(str)
        } else {
            _, err := opt.Writer.Write([]byte(fmt.Sprintf("%v", str)))
            if err != nil {
                return err
            }
        }
    case string:
        if opt.Filename == "" {
            fmt.Print(str)
        } else {
            _, err := opt.Writer.WriteString(str.(string))
            if err != nil {
                return err
            }
        }
    }
    return nil
}

func getDumpDataBody(opt* batch.DumpOption, result *batch.DumpResult, i int) error {
    first := true
    if opt.Keys {
        if err := outputOneLine(opt, result.Keys[i]); err != nil {
            return err
        }
        first = false
    }
    if opt.Values {
        if !first {
            if err := outputOneLine(opt, "\t"); err != nil {
                return err
            }
        }
        if err := outputOneLine(opt, result.Values[i]); err != nil {
            return err
        }
        first = false
    }

    if opt.Decode_key {
        if !first {
            if err := outputOneLine(opt, "\t"); err != nil {
                return err
            }
        }
        body := ""
        for j, _ := range result.Decode_keys.Attrs {
            body += fmt.Sprintf("%v", result.Decode_keys.Vecs[j][i])
            if j != len(result.Decode_keys.Attrs) - 1 {
                body += ","
            }
        }
        if err := outputOneLine(opt, body); err != nil {
            return err
        }
        first = false
    }
    
    if opt.Decode_value {
        if !first {
            if err := outputOneLine(opt, "\t"); err != nil {
                return err
            }
        }
        body := ""
        for j, _ := range result.Decode_values.Attrs {
            body += fmt.Sprintf("%v", result.Decode_values.Vecs[j][i])
            if j != len(result.Decode_values.Attrs) - 1 {
                body += ","
            }
        }
        if err := outputOneLine(opt, body); err != nil {
            return err
        }
    }
    return nil
}

func dumpTableDataToshell(opt* batch.DumpOption, result *batch.DumpResult) error {
    fmt.Println()
    header := getDumpDataHeader(opt, result)
    fmt.Println(header)

    for i := 0; i < len(result.Keys); i++ {
        err := getDumpDataBody(opt, result, i)
        if err != nil {
            return err
        }
        fmt.Println()
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
        err := getDumpDataBody(opt, result, i)
        if err != nil {
            return err
        }
        err = opt.Writer.WriteByte('\n')
        if err != nil {
            return err
        }
    }
    opt.Writer.Flush()
    return nil
}