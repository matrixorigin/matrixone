//go:build server_test
// +build server_test

package main

import (
	"os"
	"testing"
)

func TestMOServer(t *testing.T) {
	os.Args = make([]string, 2)
	os.Args[1] = "./system_vars_config.toml"
	main()
}
