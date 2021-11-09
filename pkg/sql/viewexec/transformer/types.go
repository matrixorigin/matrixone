package transformer

const (
	Sum = iota
	Avg
	Max
	Min
	Count
	StarCount
)

var TransformerNames = [...]string{
	Sum:       "sum",
	Avg:       "avg",
	Max:       "max",
	Min:       "min",
	Count:     "count",
	StarCount: "starcount",
}

var TransformerNamesMap map[string]int

type Transformer struct {
	Op    int
	Name  string
	Alias string
}
