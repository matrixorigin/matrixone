// Copyright 2021 Matrix Origin
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

package main

import (
	"fmt"
	"os"
	"strconv"
)

func main() {
	if len(os.Args) < 3 {
		fmt.Printf("Usage: gen_sql type [dbname] number\n")
		os.Exit(-1)
	}



	if os.Args[1] == "sql"{
		n,err := strconv.ParseInt(os.Args[2],10,32)
		if err != nil {
			os.Exit(-1)
		}
		generate_sql(int(n))
	} else if os.Args[1] == "sel_sql" {
		n,err := strconv.ParseInt(os.Args[3],10,32)
		if err != nil {
			os.Exit(-1)
		}
		generate_select_sql(int(n),os.Args[2])
	}
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

func generate_select_sql(cnt int,dbname string) {
	fmt.Printf("drop database if exists %s;\n",dbname)
	fmt.Printf("create database %s;\n",dbname)
	fmt.Printf("use %s;\n",dbname)
	fmt.Printf("drop table IF EXISTS A;\n")
	fmt.Printf("create table A(a int);\n")
	fmt.Printf("insert into A values (1),(2),(3),(4),(5),(6);\n")
	for i := 0; i < cnt; i++ {
		fmt.Printf("select * from A;\n")
	}
}