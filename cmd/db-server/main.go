package main

import (
	"context"
	"flag"
	"fmt"
	"github.com/pingcap/parser/terror"
	"matrixone/pkg/config"
	"matrixone/pkg/server"
	"matrixone/pkg/util/signal"
	"os"
)

const (
	nmVersion      = "V"
	nmConfig       = "config"
	nmConfigCheck  = "config-check"
	nmConfigStrict = "config-strict"
)

var (
	version      = flagBoolean(nmVersion, false, "print version information and exit")
	configPath   = flag.String(nmConfig, "", "config file path")
	configCheck  = flagBoolean(nmConfigCheck, false, "check config file validity and exit")
	configStrict = flagBoolean(nmConfigStrict, false, "enforce config file validity")
)

var (
	svr      *server.Server
	graceful bool
)

func createServer() {
	cfg := config.GetGlobalConfig()
	driver := server.NewDBDriver()
	var err error
	svr, err = server.NewServer(cfg, driver)
	terror.MustNil(err)
}

func runServer() {
	err := svr.Run()
	terror.MustNil(err)
}

func serverShutdown(isgraceful bool) {
	if isgraceful {
		graceful = true
	}
	svr.Close()
}

func registerSignalHandlers() {
	signal.SetupSignalHandler(serverShutdown)
}

func cleanup() {
	if graceful {
		svr.GracefulDown(context.Background(), nil)
	} else {
		svr.TryGracefulDown()
	}
}

func main() {
	flag.Parse()
	config.InitializeConfig(*configPath, *configCheck, *configStrict, reloadConfig, overrideConfig)
	createServer()
	registerSignalHandlers()
	runServer()
	cleanup()
	os.Exit(0)
}

func flagBoolean(name string, defaultVal bool, usage string) *bool {
	if !defaultVal {
		// Fix #4125, golang do not print default false value in usage, so we append it.
		usage = fmt.Sprintf("%s (default false)", usage)
		return flag.Bool(name, defaultVal, usage)
	}
	return flag.Bool(name, defaultVal, usage)
}

func reloadConfig(nc, c *config.Config) {
}

func overrideConfig(cfg *config.Config) {
}
