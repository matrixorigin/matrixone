package main

import (
	"fmt"
	"matrixone/pkg/config"
	"os"
)

func main() {
	if len(os.Args) != 2{
		fmt.Printf("usage: %s definitionFile \n",os.Args[0])
		return
	}

	gen := config.NewConfigurationFileGenerator(os.Args[1])
	if err := gen.Generate(); err!=nil {
		fmt.Printf("generate system variables failed. error:%v \n",err)
		os.Exit(-1)
	}
}
