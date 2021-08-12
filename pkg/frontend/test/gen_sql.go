package main

import (
	"fmt"
	"os"
	"strconv"
)

func main() {
	if len(os.Args) < 2 {
		fmt.Printf("Usage: gen_sql number\n")
		os.Exit(-1)
	}
	n,err := strconv.ParseInt(os.Args[1],10,32)
	if err != nil {
		os.Exit(-1)
	}

	//fmt.Printf("use test;\n")
	generate_sql(int(n))
}

func generate_sql(cnt int){
	for i := 0 ; i < cnt; i++ {
		fmt.Printf("drop database if exists T;\n")
		fmt.Printf("create database T;\n")
		fmt.Printf("use T;\n")
		fmt.Printf("drop table IF EXISTS A;\n")
		fmt.Printf("create table A(a int);\n")
		fmt.Printf("insert into A values (1),(2),(3),(4),(5);\n")
		fmt.Printf("insert into A values (1),(2),(3),(4),(5);\n")
	}
}