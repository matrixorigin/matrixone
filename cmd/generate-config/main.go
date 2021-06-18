package main

import (
	"fmt"
	"matrixone/pkg/config"
	"os"
)

func main() {
	argCnt := len(os.Args)
	if argCnt < 2 || argCnt > 3{
		fmt.Printf("usage: %s definitionFile [outputDiretory]\n",os.Args[0])
		return
	}

	var gen config.ConfigurationFileGenerator
	if argCnt == 2 {
		gen = config.NewConfigurationFileGenerator(os.Args[1])
	}else if argCnt == 3 {
		gen = config.NewConfigurationFileGeneratorWithOutputDirectory(os.Args[1],os.Args[2])
	}

	if err := gen.Generate(); err!=nil {
		fmt.Printf("generate system variables failed. error:%v \n",err)
		os.Exit(-1)
	}
}
