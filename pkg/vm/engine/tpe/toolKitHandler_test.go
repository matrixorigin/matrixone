package tpe

import (
	"fmt"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	vm_engine "github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tpe/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tpe/tuplecodec"
	"github.com/smartystreets/goconvey/convey"
)

func Test_getParamFromCommand(t *testing.T) {
	convey.Convey("getParamFromCommand function",t, func() {
		tpeMock, _ := engine.NewTpeEngine(&engine.TpeConfig{KVLimit: 10000})
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

        tpe_relation, ok := table.(*engine.TpeRelation)
		convey.So(ok, convey.ShouldBeTrue)

		cnt := 2
		bat := tuplecodec.MakeBatch(cnt, attrNames, attrDefs)
		lines := [][]string{{"1", "2", "-1"}, {"100", "200", "-1000"}}
		tuplecodec.FillBatch(lines, bat)

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

			err = tpe_relation.Write(uint64(cnt), bat)
			convey.So(err, convey.ShouldBeNil)

			result, err = DumpTableInfo(tpeMock, opt)
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

			err = tpe_relation.Write(uint64(cnt), bat)
			convey.So(err, convey.ShouldBeNil)

			result, err := DumpTableInfo(tpeMock, opt)
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
			convey.So(err, convey.ShouldBeNil)
			for i := 0; i < 1; i++ {
				for j := 0; j < len(attrNames); j++ {
					str := fmt.Sprintf("%v", result.Decode_values.Vecs[j + 1][i])
					convey.So(str, convey.ShouldEqual, lines[i][j])
				}
			}

		})
	})
}
