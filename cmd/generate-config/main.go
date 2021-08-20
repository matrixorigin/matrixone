package main

import (
	"bufio"
	"fmt"
	"os"
	"strings"
)

func main() {
	argCnt := len(os.Args)
	if argCnt < 2 || argCnt > 3 {
		fmt.Printf("usage: %s definitionFile [outputDiretory]\n", os.Args[0])
		return
	}

	var gen ConfigurationFileGenerator
	if argCnt == 2 {
		gen = NewConfigurationFileGenerator(os.Args[1])
	} else if argCnt == 3 {
		gen = NewConfigurationFileGeneratorWithOutputDirectory(os.Args[1], os.Args[2])
	}

	if err := gen.Generate(); err != nil {
		fmt.Printf("generate system variables failed. error:%v \n", err)
		os.Exit(-1)
	}

	file, _ := os.Open(os.Args[1])
	openFile, err := os.OpenFile("cmd/generate-config/system_vars_config.toml", os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		panic(err)
	}

	defer file.Close()
	defer openFile.Close()

	scanner := bufio.NewScanner(file)
	isOtherConfigs := false
	for scanner.Scan() {
		if !isOtherConfigs && strings.HasPrefix(scanner.Text(), "addr") {
			isOtherConfigs = true
		}
		if isOtherConfigs {
			_, err := openFile.WriteString(scanner.Text() + "\n")
			if err != nil {
				return
			}
		}
	}
}
