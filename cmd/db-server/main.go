package main

import (
	"flag"
	"fmt"
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
	serverAddress  = "localhost:6001"
)

var (
	version      = flagBoolean(nmVersion, false, "print version information and exit")
	configPath   = flag.String(nmConfig, "", "config file path")
	configCheck  = flagBoolean(nmConfigCheck, false, "check config file validity and exit")
	configStrict = flagBoolean(nmConfigStrict, false, "enforce config file validity")
)

var (
	svr      server.Server
	graceful bool
)

func createServer() {
	//cfg := config.GetGlobalConfig()
	address := fmt.Sprintf("%s:%d", config.GlobalSystemVariables.GetHost(), config.GlobalSystemVariables.GetPort())
	svr = server.NewServer(address)
}

func runServer() {
	svr.Loop()
}

func serverShutdown(isgraceful bool) {
	if isgraceful {
		graceful = true
	}
	svr.Quit()
}

func registerSignalHandlers() {
	signal.SetupSignalHandler(serverShutdown)
}

func cleanup() {
	if graceful {
		//svr.GracefulDown(context.Background(), nil)
	} else {
		//svr.TryGracefulDown()
	}
}

func main() {
	if len(os.Args) < 2 {
		fmt.Printf("Usage: %s configFile", os.Args[0])
		os.Exit(-1)
	}
	flag.Parse()
	config.InitializeConfig(*configPath, *configCheck, *configStrict, reloadConfig, overrideConfig)
	config.GlobalSystemVariables.LoadInitialValues()
	config.LoadvarsConfigFromFile(os.Args[1], &config.GlobalSystemVariables)
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
