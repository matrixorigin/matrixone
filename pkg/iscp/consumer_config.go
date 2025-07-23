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

package iscp

import (
	"bytes"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/objectio"
)

const (
	IOET_ConsumerConfig uint16 = 5000 + iota

	IOET_ConsumerConfig_V1 uint16 = 1

	IOET_ConsumerConfig_CurrVer uint16 = 1
)

func init() {
	objectio.RegisterIOEnrtyCodec(
		objectio.IOEntryHeader{
			Type:    IOET_ConsumerConfig,
			Version: IOET_ConsumerConfig_V1,
		},
		nil,
		func(b []byte) (any, error) {
			consumerConfig := &ConsumerInfo{}
			err := consumerConfig.Unmarshal(b)
			return consumerConfig, err
		},
	)
}

func UnmarshalConsumerConfig(data []byte) (*ConsumerInfo, error) {
	head := objectio.DecodeIOEntryHeader(data)
	codec := objectio.GetIOEntryCodec(*head)
	consumerConfig, err := codec.Decode(data[4:])
	if err != nil {
		return nil, err
	}
	return consumerConfig.(*ConsumerInfo), nil
}

func (c *ConsumerInfo) Marshal() (buf []byte, err error) {
	var w bytes.Buffer
	t := IOET_ConsumerConfig
	if _, err = w.Write(types.EncodeUint16(&t)); err != nil {
		return nil, err
	}
	ver := IOET_ConsumerConfig_CurrVer
	if _, err = w.Write(types.EncodeUint16(&ver)); err != nil {
		return nil, err
	}
	if _, err = w.Write(types.EncodeInt8(&c.ConsumerType)); err != nil {
		return nil, err
	}
	if _, err = objectio.WriteString(c.TableName, &w); err != nil {
		return nil, err
	}
	if _, err = objectio.WriteString(c.DbName, &w); err != nil {
		return nil, err
	}
	if _, err = objectio.WriteString(c.IndexName, &w); err != nil {
		return nil, err
	}
	return w.Bytes(), nil
}

func (c *ConsumerInfo) Unmarshal(data []byte) (err error) {
	r := bytes.NewBuffer(data)
	if _, err = r.Read(types.EncodeInt8(&c.ConsumerType)); err != nil {
		return err
	}
	if c.TableName, _, err = objectio.ReadString(r); err != nil {
		return err
	}
	if c.DbName, _, err = objectio.ReadString(r); err != nil {
		return err
	}
	if c.IndexName, _, err = objectio.ReadString(r); err != nil {
		return err
	}
	return nil
}
