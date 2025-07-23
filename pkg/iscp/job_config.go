// Copyright 2024 Matrix Origin
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

package iscp

import (
	"bytes"
	"fmt"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/objectio"
)

const (
	IOET_JobConfig_Default uint16 = 4000 + iota
	IOET_JobConfig_AlwaysUpdate
	IOET_JobConfig_Timed

	IOET_JobConfig_Default_V1      uint16 = 1
	IOET_JobConfig_AlwaysUpdate_V1 uint16 = 1
	IOET_JobConfig_Timed_V1        uint16 = 1

	IOET_JobConfig_Default_CurrVer      uint16 = 1
	IOET_JobConfig_AlwaysUpdate_CurrVer uint16 = 1
	IOET_JobConfig_Timed_CurrVer        uint16 = 1
)

func init() {
	objectio.RegisterIOEnrtyCodec(
		objectio.IOEntryHeader{
			Type:    IOET_JobConfig_Default,
			Version: IOET_JobConfig_Default_V1,
		},
		nil,
		func(b []byte) (any, error) {
			jobConfig := NewJobConfig(IOET_JobConfig_Default)
			return jobConfig, nil
		},
	)
	objectio.RegisterIOEnrtyCodec(
		objectio.IOEntryHeader{
			Type:    IOET_JobConfig_AlwaysUpdate,
			Version: IOET_JobConfig_AlwaysUpdate_V1,
		},
		nil,
		func(b []byte) (any, error) {
			jobConfig := NewJobConfig(IOET_JobConfig_AlwaysUpdate)
			return jobConfig, nil
		},
	)
	objectio.RegisterIOEnrtyCodec(
		objectio.IOEntryHeader{
			Type:    IOET_JobConfig_Timed,
			Version: IOET_JobConfig_Timed_V1,
		},
		nil,
		func(b []byte) (any, error) {
			jobConfig := NewJobConfig(IOET_JobConfig_Timed)
			err := jobConfig.Unmarshal(b)
			return jobConfig, err
		},
	)
}

func UnmarshalJobConfig(data []byte) (JobConfig, error) {
	head := objectio.DecodeIOEntryHeader(data)
	codec := objectio.GetIOEntryCodec(*head)
	jobConfig, err := codec.Decode(data[4:])
	if err != nil {
		return nil, err
	}
	return jobConfig.(JobConfig), nil
}

func NewJobConfig(jobType uint16, args ...any) JobConfig {
	switch jobType {
	case IOET_JobConfig_Default:
		return &DefaultJobConfig{}
	case IOET_JobConfig_AlwaysUpdate:
		return &AlwaysUpdateJobConfig{}
	case IOET_JobConfig_Timed:
		return &TimedJobConfig{
			UpdateInterval: args[0].(int64),
			ShareIteration: args[1].(bool),
		}
	default:
		panic(fmt.Sprintf("invalid job type: %d", jobType))
	}
}

// DefaultJobConfig is the default implementation of JobConfig.
type DefaultJobConfig struct{}

func (c *DefaultJobConfig) Marshal() (buf []byte, err error) {
	var w bytes.Buffer
	t := c.GetType()
	if _, err = w.Write(types.EncodeUint16(&t)); err != nil {
		return nil, err
	}
	ver := IOET_JobConfig_Default_CurrVer
	if _, err = w.Write(types.EncodeUint16(&ver)); err != nil {
		return nil, err
	}
	return w.Bytes(), nil
}

func (c *DefaultJobConfig) Unmarshal(data []byte) error {
	return nil
}

func (c *DefaultJobConfig) GetType() uint16 {
	return IOET_JobConfig_Default
}

// For the default job type, only other default jobs are considered.
// If the current job's watermark lags behind any other default job on the same table, update it.
// If there are no lagging jobs on the same table, all jobs update.
func (c *DefaultJobConfig) Check(
	otherConsumers []*JobEntry,
	consumer *JobEntry,
	now types.TS,
) (
	ok bool, from, to types.TS, shareIteration bool,
) {
	defaultConsumers := make([]*JobEntry, 0)
	for _, c := range otherConsumers {
		if c.jobConfig.GetType() == IOET_JobConfig_Default {
			defaultConsumers = append(defaultConsumers, c)
		}
	}
	maxTS := types.TS{}
	minTS := types.MaxTs()
	for _, c := range defaultConsumers {
		if c.watermark.GT(&maxTS) {
			maxTS = c.watermark
		}
		if c.watermark.LT(&minTS) {
			minTS = c.watermark
		}
	}
	// lag behind any other default job
	if consumer.watermark.LT(&maxTS) {
		return true, consumer.watermark, maxTS, true
	} else {
		// no lagging jobs on the same table
		if minTS.EQ(&maxTS) {
			return true, maxTS, now, true
		}
		return false, types.TS{}, types.TS{}, false
	}
}

type AlwaysUpdateJobConfig struct{}

func (c *AlwaysUpdateJobConfig) Marshal() (buf []byte, err error) {
	var w bytes.Buffer
	t := c.GetType()
	if _, err = w.Write(types.EncodeUint16(&t)); err != nil {
		return nil, err
	}
	ver := IOET_JobConfig_AlwaysUpdate_CurrVer
	if _, err = w.Write(types.EncodeUint16(&ver)); err != nil {
		return nil, err
	}
	return w.Bytes(), nil
}

func (c *AlwaysUpdateJobConfig) Unmarshal(data []byte) error {
	return nil
}

func (c *AlwaysUpdateJobConfig) GetType() uint16 {
	return IOET_JobConfig_AlwaysUpdate
}

// Always update the watermark for each job.
// Each job has its own iteration.
func (c *AlwaysUpdateJobConfig) Check(
	otherConsumers []*JobEntry,
	consumer *JobEntry,
	now types.TS,
) (
	ok bool, from, to types.TS, shareIteration bool,
) {
	return true, consumer.watermark, now, false
}

// TimedJobConfig is a job configuration that only updates when the time difference
// between now and watermark exceeds a specified duration.
type TimedJobConfig struct {
	// Duration in nanoseconds that must pass before an update is triggered
	UpdateInterval int64
	// Whether to share iteration with other jobs of the same type
	ShareIteration bool
}

func (c *TimedJobConfig) Marshal() (buf []byte, err error) {
	var w bytes.Buffer
	t := c.GetType()
	if _, err = w.Write(types.EncodeUint16(&t)); err != nil {
		return nil, err
	}
	ver := IOET_JobConfig_Timed_CurrVer
	if _, err = w.Write(types.EncodeUint16(&ver)); err != nil {
		return nil, err
	}
	interval := c.UpdateInterval
	if _, err = w.Write(types.EncodeInt64(&interval)); err != nil {
		return nil, err
	}
	share := c.ShareIteration
	if _, err = w.Write(types.EncodeBool(&share)); err != nil {
		return nil, err
	}
	return w.Bytes(), nil
}

func (c *TimedJobConfig) Unmarshal(data []byte) (err error) {
	r := bytes.NewBuffer(data)
	if _, err = r.Read(types.EncodeInt64(&c.UpdateInterval)); err != nil {
		return err
	}
	if _, err = r.Read(types.EncodeBool(&c.ShareIteration)); err != nil {
		return err
	}
	return nil
}

func (c *TimedJobConfig) GetType() uint16 {
	return IOET_JobConfig_Timed
}

// Check if the time difference between now and watermark exceeds the update interval.
// If ShareIteration is true, all timed jobs with the same configuration will share iteration.
func (c *TimedJobConfig) Check(
	otherConsumers []*JobEntry,
	consumer *JobEntry,
	now types.TS,
) (
	ok bool, from, to types.TS, shareIteration bool,
) {
	// Calculate time difference in nanoseconds
	timeDiff := now.Physical() - consumer.watermark.Physical()

	// Only update if the time difference exceeds the update interval
	if timeDiff < c.UpdateInterval {
		return false, types.TS{}, types.TS{}, false
	}

	// If sharing iteration, find other timed jobs with the same configuration
	if c.ShareIteration {
		timedConsumers := make([]*JobEntry, 0)
		for _, other := range otherConsumers {
			if other.jobConfig.GetType() == IOET_JobConfig_Timed {
				if timedConfig, ok := other.jobConfig.(*TimedJobConfig); ok {
					if timedConfig.UpdateInterval == c.UpdateInterval {
						timedConsumers = append(timedConsumers, other)
					}
				}
			}
		}

		// Find the minimum watermark among all timed jobs with the same interval
		minTS := consumer.watermark
		maxTS := consumer.watermark
		for _, tc := range timedConsumers {
			if tc.watermark.LT(&minTS) {
				minTS = tc.watermark
			}
			if tc.watermark.GT(&maxTS) {
				maxTS = tc.watermark
			}
		}
		if consumer.watermark.LT(&maxTS) {
			return true, consumer.watermark, maxTS, true
		} else {
			if minTS.EQ(&maxTS) {
				return true, maxTS, now, true
			}
			return false, types.TS{}, types.TS{}, false
		}
	}

	// Individual iteration for each job
	return true, consumer.watermark, now, false
}
