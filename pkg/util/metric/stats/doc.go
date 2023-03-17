// Copyright 2023 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

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

		reads := c.counter.Cache.Read.Swap() //NOTE: Swap()
		fields = append(fields, zap.Any("reads", reads))
		return fields
	}

 3. Initialize the LogExporter class where Stats class is initialized.

    counterLogExporter := NewCounterLogExporter(&counter.perfCounter)

 4. Register LogExporter to global stats registry using stats.Register()

	stats.Register("FamilyName", stats.WithLogExporter(counterLogExporter))
*/
