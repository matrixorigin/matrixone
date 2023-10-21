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

package v2

import "github.com/prometheus/client_golang/prometheus"

var (
	LogTailSubscriptionAmountCounter = prometheus.NewCounter(
		prometheus.CounterOpts{
			Namespace: "tn",
			Subsystem: "logtail",
			Name:      "logtail_subscription_amount_total",
			Help:      "counter for the total logtail subscription the tn have handled",
		})

	LogTailSentTrafficCounter = prometheus.NewCounter(
		prometheus.CounterOpts{
			Namespace: "tn",
			Subsystem: "logtail",
			Name:      "logtail_sent_amount_total",
			Help:      "counter for the total logtail traffic in megabytes the tn have sent",
		})
)
