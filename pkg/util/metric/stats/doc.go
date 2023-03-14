package stats

/* DefaultRegistry holds all the Mo DevStats.
   Currently, the stats Family class only contains LogExporter.

Usage:

1. Identify the Stats class to be exported. Ensure that, the Stats class uses `stats.Counter` instead of `atomic.Int64`

	type Counter struct {
		S3 struct {
			List        stats.Counter
			Head        stats.Counter
			Put         stats.Counter
			Get         stats.Counter
			Delete      stats.Counter
			DeleteMulti stats.Counter
		}
	}

 2. Create a new LogExporter class for the identified Stats class. This will be an adapter to export the Stats values in a loggable format.

	type CounterLogExporter struct {
		counter *Counter
	}

	func NewCounterLogExporter(counter *Counter) stats.LogExporter {
		return &CounterLogExporter{
			counter: counter,
		}
	}

	func (c *CounterLogExporter) Export() []zap.Field {
		var fields []zap.Field

		reads := c.counter.Cache.Read.LoadC() //NOTE: LoadC()
		fields = append(fields, zap.Any("reads", reads))
		return fields
	}

 3. Initialize the LogExporter class where Stats class is initialized.

    counterLogExporter := NewCounterLogExporter(&counter.perfCounter)

 4. Register LogExporter to global stats registry using stats.Register()

	stats.Register("FamilyName", stats.WithLogExporter(&counterLogExporter))
*/
