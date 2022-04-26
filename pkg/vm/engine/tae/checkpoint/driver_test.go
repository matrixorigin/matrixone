package checkpoint

import "testing"

type mockUnit struct{}

func (u *mockUnit) EstimateScore() int {
	return 10
}

func TestDriver(t *testing.T) {
	// driver := NewDriver()
	// driver.Start()
	// defer driver.Stop()
	// driver.OnUpdateColumn(new(mockUnit))
}
