package main

import (
	"fmt"
	"log"
	"matrixone/pkg/compress"
	"matrixone/pkg/container/types"
	"matrixone/pkg/vm/engine/logEngine"
	"matrixone/pkg/vm/metadata"
)

func main() {
	var attrs []metadata.Attribute

	db, err := logEngine.New("test.db")
	if err != nil {
		log.Fatal(err)
	}
	{
		attrs = append(attrs, metadata.Attribute{
			Alg:  compress.Lz4,
			Name: "LO_ORDERKEY",
			Type: types.Type{Oid: types.T_int64, Size: 8},
		})
		attrs = append(attrs, metadata.Attribute{
			Alg:  compress.Lz4,
			Name: "LO_LINENUMBER",
			Type: types.Type{Oid: types.T_int32, Size: 4},
		})
		attrs = append(attrs, metadata.Attribute{
			Alg:  compress.Lz4,
			Name: "LO_CUSTKEY",
			Type: types.Type{Oid: types.T_int32, Size: 4},
		})
		attrs = append(attrs, metadata.Attribute{
			Alg:  compress.Lz4,
			Name: "LO_PARTKEY",
			Type: types.Type{Oid: types.T_int32, Size: 4},
		})
		attrs = append(attrs, metadata.Attribute{
			Alg:  compress.Lz4,
			Name: "LO_SUPPKEY",
			Type: types.Type{Oid: types.T_int32, Size: 4},
		})
		attrs = append(attrs, metadata.Attribute{
			Alg:  compress.Lz4,
			Name: "LO_ORDERDATE",
			Type: types.Type{Oid: types.T_int32, Size: 4},
		})
		attrs = append(attrs, metadata.Attribute{
			Alg:  compress.Lz4,
			Name: "LO_ORDERPRIORITY",
			Type: types.Type{Oid: types.T_varchar, Size: 24},
		})
		attrs = append(attrs, metadata.Attribute{
			Alg:  compress.Lz4,
			Name: "LO_SHIPPRIORITY",
			Type: types.Type{Oid: types.T_int8, Size: 1},
		})
		attrs = append(attrs, metadata.Attribute{
			Alg:  compress.Lz4,
			Name: "LO_QUANTITY",
			Type: types.Type{Oid: types.T_float64, Size: 8},
		})
		attrs = append(attrs, metadata.Attribute{
			Alg:  compress.Lz4,
			Name: "LO_EXTENDEDPRICE",
			Type: types.Type{Oid: types.T_float64, Size: 8},
		})
		attrs = append(attrs, metadata.Attribute{
			Alg:  compress.Lz4,
			Name: "LO_ORDTOTALPRICE",
			Type: types.Type{Oid: types.T_float64, Size: 8},
		})
		attrs = append(attrs, metadata.Attribute{
			Alg:  compress.Lz4,
			Name: "LO_DISCOUNT",
			Type: types.Type{Oid: types.T_float64, Size: 8},
		})
		attrs = append(attrs, metadata.Attribute{
			Alg:  compress.Lz4,
			Name: "LO_REVENUE",
			Type: types.Type{Oid: types.T_float64, Size: 8},
		})
		attrs = append(attrs, metadata.Attribute{
			Alg:  compress.Lz4,
			Name: "LO_SUPPLYCOST",
			Type: types.Type{Oid: types.T_float64, Size: 8},
		})
		attrs = append(attrs, metadata.Attribute{
			Alg:  compress.Lz4,
			Name: "LO_TAX",
			Type: types.Type{Oid: types.T_float64, Size: 8},
		})
		attrs = append(attrs, metadata.Attribute{
			Alg:  compress.Lz4,
			Name: "LO_COMMITDATE",
			Type: types.Type{Oid: types.T_int32, Size: 4},
		})
		attrs = append(attrs, metadata.Attribute{
			Alg:  compress.Lz4,
			Name: "LO_SHIPMODE",
			Type: types.Type{Oid: types.T_varchar, Size: 24},
		})
		attrs = append(attrs, metadata.Attribute{
			Alg:  compress.Lz4,
			Name: "C_NAME",
			Type: types.Type{Oid: types.T_varchar, Size: 24},
		})
		attrs = append(attrs, metadata.Attribute{
			Alg:  compress.Lz4,
			Name: "C_ADDRESS",
			Type: types.Type{Oid: types.T_varchar, Size: 24},
		})
		attrs = append(attrs, metadata.Attribute{
			Alg:  compress.Lz4,
			Name: "C_CITY",
			Type: types.Type{Oid: types.T_varchar, Size: 24},
		})
		attrs = append(attrs, metadata.Attribute{
			Alg:  compress.Lz4,
			Name: "C_NATION",
			Type: types.Type{Oid: types.T_varchar, Size: 24},
		})
		attrs = append(attrs, metadata.Attribute{
			Alg:  compress.Lz4,
			Name: "C_REGION",
			Type: types.Type{Oid: types.T_varchar, Size: 24},
		})
		attrs = append(attrs, metadata.Attribute{
			Alg:  compress.Lz4,
			Name: "C_PHONE",
			Type: types.Type{Oid: types.T_varchar, Size: 24},
		})
		attrs = append(attrs, metadata.Attribute{
			Alg:  compress.Lz4,
			Name: "C_MKTSEGMENT",
			Type: types.Type{Oid: types.T_varchar, Size: 24},
		})
		attrs = append(attrs, metadata.Attribute{
			Alg:  compress.Lz4,
			Name: "S_NAME",
			Type: types.Type{Oid: types.T_varchar, Size: 24},
		})
		attrs = append(attrs, metadata.Attribute{
			Alg:  compress.Lz4,
			Name: "S_ADDRESS",
			Type: types.Type{Oid: types.T_varchar, Size: 24},
		})
		attrs = append(attrs, metadata.Attribute{
			Alg:  compress.Lz4,
			Name: "S_CITY",
			Type: types.Type{Oid: types.T_varchar, Size: 24},
		})
		attrs = append(attrs, metadata.Attribute{
			Alg:  compress.Lz4,
			Name: "S_NATION",
			Type: types.Type{Oid: types.T_varchar, Size: 24},
		})
		attrs = append(attrs, metadata.Attribute{
			Alg:  compress.Lz4,
			Name: "S_REGION",
			Type: types.Type{Oid: types.T_varchar, Size: 24},
		})
		attrs = append(attrs, metadata.Attribute{
			Alg:  compress.Lz4,
			Name: "S_PHONE",
			Type: types.Type{Oid: types.T_varchar, Size: 24},
		})
		attrs = append(attrs, metadata.Attribute{
			Alg:  compress.Lz4,
			Name: "P_NAME",
			Type: types.Type{Oid: types.T_varchar, Size: 24},
		})
		attrs = append(attrs, metadata.Attribute{
			Alg:  compress.Lz4,
			Name: "P_MFGR",
			Type: types.Type{Oid: types.T_varchar, Size: 24},
		})
		attrs = append(attrs, metadata.Attribute{
			Alg:  compress.Lz4,
			Name: "P_CATEGORY",
			Type: types.Type{Oid: types.T_varchar, Size: 24},
		})
		attrs = append(attrs, metadata.Attribute{
			Alg:  compress.Lz4,
			Name: "P_BRAND",
			Type: types.Type{Oid: types.T_varchar, Size: 24},
		})
		attrs = append(attrs, metadata.Attribute{
			Alg:  compress.Lz4,
			Name: "P_COLOR",
			Type: types.Type{Oid: types.T_varchar, Size: 24},
		})
		attrs = append(attrs, metadata.Attribute{
			Alg:  compress.Lz4,
			Name: "P_TYPE",
			Type: types.Type{Oid: types.T_varchar, Size: 24},
		})
		attrs = append(attrs, metadata.Attribute{
			Alg:  compress.Lz4,
			Name: "P_SIZE",
			Type: types.Type{Oid: types.T_int32, Size: 4},
		})
		attrs = append(attrs, metadata.Attribute{
			Alg:  compress.Lz4,
			Name: "P_CONTAINER",
			Type: types.Type{Oid: types.T_varchar, Size: 24},
		})
	}
	if err := db.Create("lineorder_flat", attrs); err != nil {
		log.Fatal(err)
	}
	fmt.Printf("create table lineorder_flat success\n")
}
