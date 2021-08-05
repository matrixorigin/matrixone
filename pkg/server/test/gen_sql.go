package main

import "fmt"

func main() {
	//fmt.Printf("use test;\n")
	generate_sql(100)
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
