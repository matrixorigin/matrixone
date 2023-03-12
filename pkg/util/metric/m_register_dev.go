package metric

type Snapshot map[string]int64

type Collectable interface {
	Collect() Snapshot
}

type StatsRegistry struct {
	registry map[string]*Collectable
}

var DefaultStatsRegistry = StatsRegistry{}

func (r *StatsRegistry) RegisterDevMetric(statsFamilyName string, stats *Collectable) {
	if _, exists := r.registry[statsFamilyName]; exists {
		panic("Family Name is already registered ")
	}
	r.registry[statsFamilyName] = stats
}

func (r *StatsRegistry) Gather() (snapshot map[string]Snapshot) {
	for statsFamilyName, stats := range r.registry {
		snapshot[statsFamilyName] = (*stats).Collect()
	}
	return
}
