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

package cdc

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/matrixorigin/matrixone/pkg/sql/plan/tools"
)

func RunDecoder(
	ctx context.Context,
	inQueue Queue[tools.Pair[*TableCtx, *DecoderInput]],
	outQueue Queue[tools.Pair[*TableCtx, *DecoderOutput]],
	codec Decoder) {
	for {
		select {
		case <-ctx.Done():
			break
		default:
			//TODO: refine
			if inQueue.Size() != 0 {
				head := inQueue.Front()
				inQueue.Pop()
				res := codec.Decode(head.Key, head.Value)
				outQueue.Push(tools.NewPair[*TableCtx, *DecoderOutput](head.Key, res))
			} else {
				time.Sleep(time.Millisecond * 100)
			}
		}
	}
}

func RunSinker(
	ctx context.Context,
	inQueue Queue[tools.Pair[*TableCtx, *DecoderOutput]],
	sinker Sinker,
) {
	for {
		select {
		case <-ctx.Done():
			break
		default:
			if inQueue.Size() != 0 {
				head := inQueue.Front()
				inQueue.Pop()
				err := sinker.Sink(head.Key, head.Value)
				if err != nil {
					fmt.Fprintln(os.Stderr, "sinker.Sink error", err)
				}
			} else {
				time.Sleep(time.Millisecond * 100)
			}
		}
	}
}
