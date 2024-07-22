package embed

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"
)

// basePort         = 18000
// 	baseFrontendPort = 16001
// 	baseUnixSocket   = 0

type state int

const (
	stopped = state(0)
	started = state(1)
)

type cluster struct {
	sync.RWMutex

	services map[string]ServiceOperator

	options struct {
		dataPath  string
		cn        int
		withProxy bool
		adjust    func(ServiceOperator)
	}

	gen struct {
		basePort         int
		baseFrontendPort int
	}
}

func NewSingleCN(
	opts ...Option,
) (Cluster, error) {
	return NewMultiCN(1, opts...)
}

func NewMultiCN(
	cn int,
	opts ...Option,
) (Cluster, error) {
	opts = append(
		opts,
		func(c *cluster) {
			c.options.cn = cn
		},
	)
	return NewCluster(opts...)
}

func NewCluster(
	opts ...Option,
) (Cluster, error) {
	c := &cluster{
		services: map[string]ServiceOperator{},
	}
	for _, opt := range opts {
		opt(c)
	}
	c.adjust()
	return nil, nil
}

func newClusterWithConfigFile(
	serviceConfigs []string,
) (Cluster, error) {
	c := &cluster{
		services: make(map[string]ServiceOperator),
	}
	for _, f := range serviceConfigs {
		s, err := newService(f)
		if err != nil {
			return nil, err
		}
		c.services[s.ServiceID()] = s
	}
	return nil, nil
}

func (c *cluster) adjust() {
	c.gen.baseFrontendPort = 6001
	c.gen.basePort = 18000

	if c.options.cn == 0 {
		c.options.cn = 1
	}
	if c.options.dataPath == "" {
		c.options.dataPath = filepath.Join("/tmp/", fmt.Sprintf("%d", time.Now().Nanosecond()))
		if err := os.MkdirAll(c.options.dataPath, 0755); err != nil {
			panic(err)
		}
	}

	if c.options.withProxy ||
		c.options.cn > 1 {
		c.gen.baseFrontendPort = 16001
	}
}

func (c *cluster) initConfigs() error {
	if err := c.initLogServiceConfig(); err != nil {
		return err
	}

	if err := c.initTNServiceConfig(); err != nil {
		return err
	}

	if err := c.initCNServiceConfig(); err != nil {
		return err
	}

	return nil
}

func (c *cluster) initLogServiceConfig() error {
	return genConfig(
		filepath.Join(c.options.dataPath, "log.toml"),
		fmt.Sprintf(
			logConfig,
			c.options.dataPath,
		))
}

func (c *cluster) initTNServiceConfig() error {
	return genConfig(
		filepath.Join(c.options.dataPath, "tn.toml"),
		fmt.Sprintf(
			tnConfig,
			c.options.dataPath,
			c.options.dataPath,
			c.options.dataPath,
			c.getNextBasePort(),
		))
}

func (c *cluster) initCNServiceConfig() error {
	for i := 0; i < c.options.cn; i++ {
		err := genConfig(
			filepath.Join(c.options.dataPath, fmt.Sprintf("cn-%d.toml", i)),
			fmt.Sprintf(
				cnConfig,
				c.options.dataPath,
				c.options.dataPath,
				c.options.dataPath,
				i,
				c.getNextBasePort(),
				i,
				c.getNextFrontPort(),
				c.options.dataPath,
				i,
			))
		if err != nil {
			return err
		}
	}
	return nil
}

func (c *cluster) getNextBasePort() int {
	v := c.gen.basePort
	c.gen.basePort += 100
	return v
}

func (c *cluster) getNextFrontPort() int {
	v := c.gen.baseFrontendPort
	c.gen.baseFrontendPort++
	return v
}

func genConfig(
	file string,
	cfg string,
) error {
	f, err := os.Create(file)
	if err != nil {
		return err
	}
	if _, err := f.WriteString(cfg); err != nil {
		return err
	}
	if err := f.Sync(); err != nil {
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	return nil
}
