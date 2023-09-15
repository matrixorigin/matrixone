package cnservice

import (
	"testing"
)

func TestDefaultCnConfig(t *testing.T) {
	cfg := Config{}
	cfg.SetDefaultValue()
}
