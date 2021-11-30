// Copyright 2021 Matrix Origin
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

package driver

import (
	"github.com/fagongzi/goetty/codec"
	"github.com/fagongzi/util/protoc"
	"github.com/matrixorigin/matrixcube/server"
	"github.com/matrixorigin/matrixone/pkg/vm/driver/pb"
)


//BuildRequest build the request according to the command
func (h *driver) BuildRequest(req *server.CustomRequest, cmd interface{}) error {
	req.Args = cmd
	customReq := cmd.(pb.Request)
	switch customReq.Type {
	case pb.Set:
		msg := customReq.Set
		req.Key = msg.Key
		req.Group = uint64(customReq.Group)
		req.CustomType = uint64(pb.Set)
		req.Write = true
		req.Cmd = protoc.MustMarshal(&msg)
	case pb.SetIfNotExist:
		msg := customReq.Set
		req.Key = msg.Key
		req.Group = uint64(customReq.Group)
		req.CustomType = uint64(pb.SetIfNotExist)
		req.Write = true
		req.Cmd = protoc.MustMarshal(&msg)
	case pb.Del:
		msg := customReq.Delete
		req.Key = msg.Key
		req.Group = uint64(customReq.Group)
		req.CustomType = uint64(pb.Del)
		req.Write = true
	case pb.DelIfNotExist:
		msg := customReq.Delete
		req.Key = msg.Key
		req.Group = uint64(customReq.Group)
		req.CustomType = uint64(pb.DelIfNotExist)
		req.Write = true
	case pb.Get:
		msg := customReq.Get
		req.Key = msg.Key
		req.Group = uint64(customReq.Group)
		req.CustomType = uint64(pb.Get)
		req.Read = true
	case pb.PrefixScan:
		msg := customReq.PrefixScan
		req.Key = msg.StartKey
		req.Group = uint64(customReq.Group)
		req.CustomType = uint64(pb.PrefixScan)
		req.Read = true
		req.Cmd = protoc.MustMarshal(&msg)
	case pb.Scan:
		msg := customReq.Scan
		req.Key = msg.Start
		req.Group = uint64(customReq.Group)
		req.CustomType = uint64(pb.Scan)
		req.Read = true
		req.Cmd = protoc.MustMarshal(&msg)
	case pb.Incr:
		msg := customReq.AllocID
		req.Key = msg.Key
		req.Group = uint64(customReq.Group)
		req.CustomType = uint64(pb.Incr)
		req.Write = true
		req.Cmd = protoc.MustMarshal(&msg)
	case pb.CreateTablet:
		msg := customReq.CreateTablet
		req.Group = uint64(customReq.Group)
		req.CustomType = uint64(pb.CreateTablet)
		req.Write = true
		req.Cmd = protoc.MustMarshal(&msg)
	case pb.DropTablet:
		msg := customReq.DropTablet
		req.Group = uint64(customReq.Group)
		req.CustomType = uint64(pb.DropTablet)
		req.Write = true
		req.Cmd = protoc.MustMarshal(&msg)
	case pb.Append:
		msg := customReq.Append
		req.Group = uint64(customReq.Group)
		req.CustomType = uint64(pb.Append)
		req.Write = true
		req.Cmd = protoc.MustMarshal(&msg)
	case pb.TabletNames:
		msg := customReq.TabletIds
		req.Group = uint64(customReq.Group)
		req.CustomType = uint64(pb.TabletNames)
		req.Read = true
		req.Cmd = protoc.MustMarshal(&msg)
	case pb.GetSegmentIds:
		msg := customReq.GetSegmentIds
		req.Group = uint64(customReq.Group)
		req.CustomType = uint64(pb.GetSegmentIds)
		req.Read = true
		req.Cmd = protoc.MustMarshal(&msg)
	case pb.GetSegmentedId:
		msg := customReq.GetSegmentedId
		req.Group = uint64(customReq.Group)
		req.CustomType = uint64(pb.GetSegmentedId)
		req.Read = true
		req.Cmd = protoc.MustMarshal(&msg)
	}
	return nil
}

func (h *driver) Codec() (codec.Encoder, codec.Decoder) {
	return nil, nil
}
