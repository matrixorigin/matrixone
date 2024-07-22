package embed

func WithConfigs(
	configs []string,
) Option {
	return func(c *cluster) {
		c.files = configs
	}
}
