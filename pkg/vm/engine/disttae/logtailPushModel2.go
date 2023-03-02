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

package disttae

import (
	"context"
	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/pb/logtail"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"time"
)

func (e *Engine) ParallelToReceiveTableLogTail() {
	go func() {
		for {
			// new parallelNums routine to consume log tails.
			errChan := make(chan error, parallelNums)
			receiver := make([]routineController, parallelNums)
			for i := range receiver {
				receiver[i] = createRoutineToConsumeLogTails(i, bufferLength, e, errChan)
			}

			ctx := context.TODO()
			ch := make(chan logTailSubscriberResponse, 1)
			// a dead loop to receive log, if lost connect, should reconnect.
			for {
				deadline, cancel := context.WithTimeout(ctx, maxTimeToWaitServerResponse)
				select {
				case <-deadline.Done():
					// max wait time is out.
					goto cleanAndReconnect

				case ch <- e.subscriber.receiveResponse():
					// receive a response from log tail service.
					cancel()

				case err := <-errChan:
					// receive an error from sub-routine to consume log.
					logutil.ErrorField(err)
					goto cleanAndReconnect
				}

				resp := <-ch
				if resp.err != nil {
					// may rpc close error or decode error.
					logutil.ErrorField(resp.err)
					goto cleanAndReconnect
				}

				response := resp.response
				// consume subscribe response
				if sResponse := response.GetSubscribeResponse(); sResponse != nil {
					if err := distributeSubscribeResponse(
						ctx, e, sResponse, receiver); err != nil {
						logutil.ErrorField(err)
						goto cleanAndReconnect
					}
					continue
				}

				// consume update response
				if uResponse := response.GetUpdateResponse(); uResponse != nil {
					if err := distributeUpdateResponse(
						ctx, e, uResponse, receiver); err != nil {
						logutil.ErrorField(err)
						goto cleanAndReconnect
					}
					continue
				}
			}

		cleanAndReconnect:
			for _, r := range receiver {
				r.close()
			}
			e.receiveLogTailTime.initLogTailTimestamp()
			e.subscribed.initTableSubscribeRecord()
			for {
				if err := e.initTableLogTailSubscriber(); err != nil {
					logutil.Error("rebuild the cn log tail client failed.")
					continue
				}
				if err := e.firstTimeConnectToLogTailServer(ctx); err == nil {
					logutil.Info("reconnect to dn log tail server succeed.")
					break
				}
				logutil.Error("reconnect to dn log tail server failed.")
				time.Sleep(periodToReconnectDnLogServer)
			}
		}
	}()
}

func ifShouldNotDistribute(dbId, tblId uint64) bool {
	return dbId == catalog.MO_CATALOG_ID && tblId <= catalog.MO_COLUMNS_ID
}

func distributeSubscribeResponse(
	ctx context.Context,
	e *Engine,
	response *logtail.SubscribeResponse,
	recRoutines []routineController) error {
	lt := response.Logtail
	tbl := lt.GetTable()
	notDistribute := ifShouldNotDistribute(tbl.DbId, tbl.TbId)
	if notDistribute {
		if err := e.consumeSubscribeResponse(ctx, response); err != nil {
			return err
		}
		e.subscribed.setTableSubscribe(tbl.DbId, tbl.TbId)
	} else {
		routineIndex := tbl.TbId % parallelNums
		recRoutines[routineIndex].sendSubscribeResponse(response)
	}
	// no matter how we consume the response, should update all timestamp.
	e.receiveLogTailTime.updateTimestamp(parallelNums, *lt.Ts)
	for _, rc := range recRoutines {
		rc.updateTimeFromT(*lt.Ts)
	}
	return nil
}

func distributeUpdateResponse(
	ctx context.Context,
	e *Engine,
	response *logtail.UpdateResponse,
	recRoutines []routineController) error {
	list := response.GetLogtailList()
	logList(list).Sort()

	// after sort, the smaller tblId, the smaller the index.
	var index int
	for index = 0; index < len(list); index++ {
		table := list[index].Table
		notDistribute := ifShouldNotDistribute(table.DbId, table.TbId)
		if !notDistribute {
			break
		}
		if err := e.consumeUpdateLogTail(ctx, list[index]); err != nil {
			return err
		}
	}
	for ; index < len(list); index++ {
		table := list[index].Table
		recIndex := table.TbId % parallelNums
		recRoutines[recIndex].sendTableLogTail(list[index])
	}
	// should update all the timestamp.
	e.receiveLogTailTime.updateTimestamp(parallelNums, *response.To)
	for _, rc := range recRoutines {
		rc.updateTimeFromT(*response.To)
	}
	return nil
}

type routineController struct {
	ctx        context.Context
	routineId  int
	closeChan  chan bool
	signalChan chan routineControlCmd
}

func (rc *routineController) sendSubscribeResponse(r *logtail.SubscribeResponse) {
	rc.signalChan <- cmdToConsumeSub{log: r}
}

func (rc *routineController) sendTableLogTail(r logtail.TableLogtail) {
	rc.signalChan <- cmdToConsumeLog{log: r}
}

func (rc *routineController) updateTimeFromT(t timestamp.Timestamp) {
	rc.signalChan <- cmdToUpdateTime{time: t}
}

func (rc *routineController) close() {
	rc.closeChan <- true
}

func createRoutineToConsumeLogTails(
	routineId int, signalBufferLength int,
	e *Engine, errOut chan error) routineController {
	controller := routineController{
		ctx:        context.TODO(),
		routineId:  routineId,
		closeChan:  make(chan bool),
		signalChan: make(chan routineControlCmd, signalBufferLength),
	}

	go func(engine *Engine, receiver *routineController, errRet chan error) {
		for {
			select {
			case cmd := <-receiver.signalChan:
				if err := cmd.action(engine, receiver); err != nil {
					errRet <- err
				}

			case <-receiver.closeChan:
				close(receiver.closeChan)
				close(receiver.signalChan)
				return
			}
		}
	}(e, &controller, errOut)

	return controller
}

// a signal to control the routine which is responsible for consuming log tail.
type routineControlCmd interface {
	action(e *Engine, ctrl *routineController) error
}

type cmdToConsumeSub struct{ log *logtail.SubscribeResponse }
type cmdToConsumeLog struct{ log logtail.TableLogtail }
type cmdToUpdateTime struct{ time timestamp.Timestamp }

func (cmd cmdToConsumeSub) action(e *Engine, ctrl *routineController) error {
	response := cmd.log
	if err := e.consumeSubscribeResponse(ctrl.ctx, response); err != nil {
		return err
	}
	lt := response.GetLogtail()
	tbl := lt.GetTable()
	e.subscribed.setTableSubscribe(tbl.DbId, tbl.TbId)
	return nil
}

func (cmd cmdToConsumeLog) action(e *Engine, ctrl *routineController) error {
	response := cmd.log
	if err := e.consumeUpdateLogTail(ctrl.ctx, response); err != nil {
		return err
	}
	return nil
}

func (cmd cmdToUpdateTime) action(e *Engine, ctrl *routineController) error {
	e.receiveLogTailTime.updateTimestamp(ctrl.routineId, cmd.time)
	return nil
}

func (e *Engine) consumeSubscribeResponse(ctx context.Context, rp *logtail.SubscribeResponse) error {
	lt := rp.GetLogtail()
	return updatePartitionOfPush(ctx, e.subscriber.dnNodeID, e, &lt, *lt.Ts)
}

func (e *Engine) consumeUpdateLogTail(ctx context.Context, rp logtail.TableLogtail) error {
	return updatePartitionOfPush(ctx, e.subscriber.dnNodeID, e, &rp, *rp.Ts)
}
