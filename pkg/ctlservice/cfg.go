package ctlservice

// Config ctl service address
type Config struct {
	// ListenAddress ctl service listen address for receiving ctl requests
	ListenAddress string `toml:"listen-address"`
	// ServiceAddress service address for communication, if this address is not set, use
	// ListenAddress as the communication address.
	ServiceAddress string `toml:"service-address"`
}

// Adjust adjust config, setup default configs
func (c *Config) Adjust(defaultListenAddress string) {
	if c.ListenAddress == "" {
		c.ListenAddress = defaultListenAddress
	}
	if c.ServiceAddress == "" {
		c.ServiceAddress = c.ListenAddress
	}
}
