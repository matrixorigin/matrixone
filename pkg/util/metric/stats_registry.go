package metric

// Stats holds counter name and counter value of stats.
type Stats map[string]int64

// StatsFamily holds family name, and stats snapshot
type StatsFamily struct {
	name  string
	stats Stats
}

// CollectableStats interface is implemented by all stats that will register to DefaultStatsRegistry
type CollectableStats interface {
	Collect() Stats
}

// StatsRegistry holds the CollectableStats objects.
type StatsRegistry struct {
	registry map[string]*CollectableStats
}

// DefaultStatsRegistry will be used for registering default developer stats
var DefaultStatsRegistry = StatsRegistry{}

// RegisterStats registers stats family to registry
// statsFName is the family name of the stats
// stats is the pointer to the stats object
func (r *StatsRegistry) RegisterStats(statsFName string, stats *CollectableStats) {
	if _, exists := r.registry[statsFName]; exists {
		panic("Family Name is already registered ")
	}
	r.registry[statsFName] = stats
}

// Gather returns the snapshot of all the CollectableStats registered to StatsRegistry
func (r *StatsRegistry) Gather() (statsFamilies []*StatsFamily) {
	for statsFName, stats := range r.registry {
		statsFamily := &StatsFamily{
			name:  statsFName,
			stats: (*stats).Collect(),
		}
		statsFamilies = append(statsFamilies, statsFamily)
	}
	return
}
