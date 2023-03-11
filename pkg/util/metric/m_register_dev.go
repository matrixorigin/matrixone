package metric

type DevCounter interface {
	Collect() map[string]int64
}

var DefaultDevMetricRegistry = make(map[string]*DevCounter)

func RegisterDevMetric(familyName string, family *DevCounter) {
	if _, exists := DefaultDevMetricRegistry[familyName]; exists {
		panic("Family Name is already registered ")
	}
	DefaultDevMetricRegistry[familyName] = family
}
