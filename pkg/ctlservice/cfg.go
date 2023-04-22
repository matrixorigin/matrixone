package ctlservice

import (
	"github.com/matrixorigin/matrixone/pkg/util/address"
)

// Config ctl service address
type Config struct {
	// Address ctl service address
	Address address.Address `toml:"address"`
}

// Adjust adjust config, setup default configs
func (c *Config) Adjust(machineHost, defaultListenAddress string) {
	c.Address.Adjust(machineHost, defaultListenAddress)
}
