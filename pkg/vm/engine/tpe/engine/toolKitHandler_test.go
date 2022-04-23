package engine

import (
	"errors"
	"fmt"
	"os"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	vm_engine "github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tpe/tuplecodec"
	"github.com/prashantv/gostub"
	"github.com/smartystreets/goconvey/convey"
)

func Test_getParamFromCommand(t *testing.T) {
	convey.Convey("getParamFromCommand function",t, func() {
		tpeMock, _ := NewTpeEngine(&TpeConfig{
			KvType:                    tuplecodec.KV_MEMORY,
			SerialType:                tuplecodec.ST_JSON,
			ValueLayoutSerializerType: "default",
			KVLimit:                   10000,
		})
		if tpeMock == nil {
			return
		}
		err := tpeMock.Create(0, "ssb", 0)
		convey.So(err, convey.ShouldBeNil)

		dbDesc, err := tpeMock.Database("ssb")
		convey.So(err,convey.ShouldBeNil)

		_, attrDefs := tuplecodec.MakeAttributes(types.T_uint64,types.T_uint64,types.T_int64)
		attrNames := []string{
			"a","b","c",
		}
		var defs []vm_engine.TableDef
		for i, def := range attrDefs {
			def.Attr.Name = attrNames[i]
			defs = append(defs,def)
		}
	
		defs = append(defs,&vm_engine.CommentDef{
			Comment: "A(a,b,c)",
		})

		err = dbDesc.Create(0, "t1", defs)
		convey.So(err,convey.ShouldBeNil)

		table, err := dbDesc.Relation("t1")
        convey.So(err, convey.ShouldBeNil)

        tpe_relation, ok := table.(*TpeRelation)
		convey.So(ok, convey.ShouldBeTrue)

		cnt := 2
		bat := tuplecodec.MakeBatch(cnt, attrNames, attrDefs)
		lines := [][]string{{"1", "2", "-1"}, {"100", "200", "-1000"}}
		tuplecodec.FillBatch(lines, bat)

		convey.Convey("Test DumpDatabaseInfo", func() {
			args := []string{"system_vars_config.toml"}
			err = DumpDatabaseInfo(tpeMock, args)
			convey.So(err, convey.ShouldBeNil)

			args = []string{"system_vars_config.toml", "-db"}
			err = DumpDatabaseInfo(tpeMock, args)
			convey.So(err, convey.ShouldResemble, errors.New("the dbname do not input"))

			args = []string{"system_vars_config.toml", "-table"}
			err = DumpDatabaseInfo(tpeMock, args)
			convey.So(err, convey.ShouldResemble, errors.New("the table do not input"))

			args = []string{"system_vars_config.toml", "-limit"}
			err = DumpDatabaseInfo(tpeMock, args)
			convey.So(err, convey.ShouldResemble, errors.New("the limit param do not input"))

			args = []string{"system_vars_config.toml", "-limit", "1"}
			err = DumpDatabaseInfo(tpeMock, args)
			convey.So(err, convey.ShouldResemble, errors.New("the input limit param format is not correcr"))

			args = []string{"system_vars_config.toml", "-export"}
			err = DumpDatabaseInfo(tpeMock, args)
			convey.So(err, convey.ShouldResemble, errors.New("the export filename do not input"))

			args = []string{"system_vars_config.toml", "-getvalueofkey"}
			err = DumpDatabaseInfo(tpeMock, args)
			convey.So(err, convey.ShouldResemble, errors.New("the getValueofKey param do not input"))

			args = []string{"system_vars_config.toml", "-getvalueoforiginkey"}
			err = DumpDatabaseInfo(tpeMock, args)
			convey.So(err, convey.ShouldResemble, errors.New("the getValueofOriginKey param do not input"))

			args = []string{"system_vars_config.toml", "-abc"}
			err = DumpDatabaseInfo(tpeMock, args)
			convey.So(err, convey.ShouldResemble, errors.New("Unmatch keyword, please check the input"))

			args = []string{"system_vars_config.toml", "-keys", "-values", "-decode_key", "-decode_value", "-limit", "0,2"}
			err = DumpDatabaseInfo(nil, args)
			convey.So(err, convey.ShouldResemble, errors.New("TpeEngine convert failed"))

			err = DumpDatabaseInfo(tpeMock, args)
			convey.So(err, convey.ShouldResemble, errors.New("The limit range is out of the result"))

			args = []string{"system_vars_config.toml", "-keys", "-values", "-decode_key", "-decode_value", "-db", "ssb", "-table", "t1", "-export", "a.txt"}
			stubs := gostub.StubFunc(&DumpTableInfo, nil, errors.New("can not open file"))
			defer stubs.Reset()
			err = DumpDatabaseInfo(tpeMock, args)
			convey.So(err, convey.ShouldNotBeNil)

			stubs = gostub.StubFunc(&DumpTableInfo, nil, nil)
			err = DumpDatabaseInfo(tpeMock, args)
			convey.So(err, convey.ShouldBeNil)

			err = os.Remove("a.txt")
			convey.So(err, convey.ShouldBeNil)
		})

		convey.Convey("GetAllvalues of table", func() {
			args := []string{"system_vars_config.toml", "-keys", "-values", "-decode_key", "-decode_value", "-db", "ssb", "-table", "t1"}
			opt := &batch.DumpOption{}
			err = getParamFromCommand(opt, args)
			convey.So(err, convey.ShouldBeNil)
			convey.So(opt.Db_name, convey.ShouldResemble, []string{"ssb"})
			convey.So(opt.Table_name, convey.ShouldResemble, []string{"t1"})
			convey.So(opt.Keys, convey.ShouldBeTrue)
			convey.So(opt.Values, convey.ShouldBeTrue)
			convey.So(opt.Decode_key, convey.ShouldBeTrue)
			convey.So(opt.Decode_value, convey.ShouldBeTrue)
			convey.So(opt.UseKey, convey.ShouldBeFalse)
			convey.So(opt.PrimaryKey, convey.ShouldBeNil)
			convey.So(opt.UseValue, convey.ShouldBeFalse)
			convey.So(opt.PrimaryValue, convey.ShouldBeNil)

			result, err := DumpTableInfo(tpeMock, opt)
			convey.So(err, convey.ShouldBeNil)
			convey.So(result, convey.ShouldNotBeNil)
			convey.So(result.Keys, convey.ShouldBeNil)
			convey.So(result.Values, convey.ShouldBeNil)
			convey.So(result.Decode_keys.Attrs, convey.ShouldNotBeNil)
			convey.So(result.Decode_keys.Vecs, convey.ShouldResemble, []batch.DumpDecodeItem{batch.DumpDecodeItem(nil)})
			attr := append(result.Decode_keys.Attrs, attrNames...)
			convey.So(result.Decode_values.Attrs, convey.ShouldResemble, attr)
			convey.So(result.Decode_values.Vecs, convey.ShouldResemble, []batch.DumpDecodeItem{batch.DumpDecodeItem(nil), 
						batch.DumpDecodeItem(nil), batch.DumpDecodeItem(nil), batch.DumpDecodeItem(nil)})

			header := "Keys" + "\t\t\t\t" + "Values" + "\t\t\t\t" + fmt.Sprintf("%v", result.Decode_keys.Attrs) + 
							"\t\t\t\t" + fmt.Sprintf("%v", result.Decode_values.Attrs)
			header2 := getDumpDataHeader(opt, result)
			convey.So(header, convey.ShouldEqual, header2)

			bat.Zs = nil
			err = tpe_relation.Write(uint64(cnt), bat)
			convey.So(err, convey.ShouldBeNil)

			result, err = DumpTableInfo(tpeMock, opt)
			convey.So(result, convey.ShouldNotBeNil)
			convey.So(err, convey.ShouldBeNil)
			for i := 0; i < cnt; i++ {
				for j := 0; j < len(attrNames); j++ {
					str := fmt.Sprintf("%v", result.Decode_values.Vecs[j + 1][i])
					convey.So(str, convey.ShouldEqual, lines[i][j])
				}
			}
		})

		convey.Convey("GetAllvalues for designated key of table", func() {
			args := []string{"system_vars_config.toml", "-keys", "-values", "-decode_key", "-decode_value", "-db", "ssb", "-table", "t1", 
						"-getValueofKey", "139,139,137", "-limit", "0,1"}
			opt := &batch.DumpOption{}
			err = getParamFromCommand(opt, args)
			convey.So(err, convey.ShouldBeNil)
			convey.So(opt.Limit, convey.ShouldResemble, []uint64{0, 1})
			convey.So(opt.UseKey, convey.ShouldBeTrue)
			convey.So(opt.PrimaryKey, convey.ShouldResemble, []byte{139, 139, 137})
			convey.So(opt.UseValue, convey.ShouldBeFalse)
			convey.So(opt.PrimaryValue, convey.ShouldBeNil)

			bat.Zs = nil
			err = tpe_relation.Write(uint64(cnt), bat)
			convey.So(err, convey.ShouldBeNil)

			result, err := DumpTableInfo(tpeMock, opt)
			convey.So(result, convey.ShouldNotBeNil)
			convey.So(err, convey.ShouldBeNil)
			for i := 0; i < 1; i++ {
				for j := 0; j < len(attrNames); j++ {
					str := fmt.Sprintf("%v", result.Decode_values.Vecs[j + 1][i])
					convey.So(str, convey.ShouldEqual, lines[i][j])
				}
			}

			args = []string{"system_vars_config.toml", "-keys", "-values", "-decode_key", "-decode_value", "-db", "ssb", "-table", "t1", 
						"-getValueofOriginKey", "1234"}
			opt = &batch.DumpOption{}
			err = getParamFromCommand(opt, args)
			convey.So(err, convey.ShouldBeNil)
			convey.So(opt.UseKey, convey.ShouldBeFalse)
			convey.So(opt.PrimaryKey, convey.ShouldBeNil)
			convey.So(opt.UseValue, convey.ShouldBeTrue)
			convey.So(opt.PrimaryValue, convey.ShouldResemble, []string{"1234"})

			opt.PrimaryValue[0] = fmt.Sprintf("%v", result.Decode_values.Vecs[0][0])
			result, err = DumpTableInfo(tpeMock, opt)
			convey.So(result, convey.ShouldNotBeNil)
			convey.So(err, convey.ShouldBeNil)
			for i := 0; i < 1; i++ {
				for j := 0; j < len(attrNames); j++ {
					str := fmt.Sprintf("%v", result.Decode_values.Vecs[j + 1][i])
					convey.So(str, convey.ShouldEqual, lines[i][j])
				}
			}

		})
	
		err = dbDesc.Delete(0, "t1")
		convey.So(err, convey.ShouldBeNil)
	})
}
