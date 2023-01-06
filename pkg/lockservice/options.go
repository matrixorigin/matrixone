// Copyright 2022 Matrix Origin
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

package lockservice

// WithGranularity set rows granularity, the default granularity is Row.
func (opts LockOptions) WithGranularity(granularity Granularity) LockOptions {
	opts.granularity = granularity
	return opts
}

// WithMode set lock mode, the default mode is Exclusive.
func (opts LockOptions) WithMode(mode LockMode) LockOptions {
	opts.mode = mode
	return opts
}

// WithWaitPolicy set wait policy, the default policy is Wait.
func (opts LockOptions) WithWaitPolicy(policy WaitPolicy) LockOptions {
	opts.policy = policy
	return opts
}
