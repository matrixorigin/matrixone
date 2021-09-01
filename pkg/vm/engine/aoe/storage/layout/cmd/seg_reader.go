package main

import (
	"matrixone/pkg/logutil"
	"os"
)

func main() {
	args := os.Args
	if len(args) != 2 {
		logutil.Errorf("invalid arg")
	}
	filename := args[1]
	logutil.Infof("%s", filename)
	panic("")
}
