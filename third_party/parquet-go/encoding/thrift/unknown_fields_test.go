package thrift_test

import (
	"bytes"
	"testing"

	"github.com/parquet-go/parquet-go/encoding/thrift"
)

func TestUnknownBooleanFieldsAndCollections(t *testing.T) {
	type inner struct {
		A bool  `thrift:"1"`
		B bool  `thrift:"2"`
		N int32 `thrift:"3"`
	}
	type input struct {
		A        bool              `thrift:"1"`
		B        bool              `thrift:"2"`
		Inner    inner             `thrift:"3"`
		List     []bool            `thrift:"4"`
		Set      map[bool]struct{} `thrift:"5"`
		Map      map[bool]bool     `thrift:"6"`
		Sentinel string            `thrift:"7"`
	}
	type output struct {
		Sentinel string `thrift:"7"`
	}
	value := input{true, false, inner{true, false, 42}, []bool{true, false}, map[bool]struct{}{true: {}, false: {}}, map[bool]bool{true: false, false: true}, "sentinel"}
	for name, p := range map[string]thrift.Protocol{"compact": &thrift.CompactProtocol{}, "binary": &thrift.BinaryProtocol{}} {
		t.Run(name, func(t *testing.T) {
			b, err := thrift.Marshal(p, &value)
			if err != nil {
				t.Fatal(err)
			}
			r := bytes.NewReader(b)
			var got output
			if err := thrift.NewDecoder(p.NewReader(r)).Decode(&got); err != nil {
				t.Fatal(err)
			}
			if got.Sentinel != value.Sentinel || r.Len() != 0 {
				t.Fatalf("got %q, %d bytes remaining", got.Sentinel, r.Len())
			}
			if err := thrift.Unmarshal(p, b[:len(b)/2], &got); err == nil {
				t.Fatal("truncated collection decoded successfully")
			}
		})
	}
}
