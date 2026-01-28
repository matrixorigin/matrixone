# Remote Run Implementation

**Last Updated:** 2025-01-27

## Overview

Remote Run is a core mechanism in MatrixOne for distributed query execution. It allows pipelines to be sent to remote CN nodes for execution, enabling load balancing and parallel processing.

## Architecture

### Components

1. **Client Side**: Sends pipeline to remote nodes
   - Location: `pkg/sql/compile/remoterunClient.go`
   - Main function: `Scope.remoteRun()`

2. **Server Side**: Receives and executes pipeline
   - Location: `pkg/sql/compile/remoterunServer.go`
   - Main functions: `CnServerMessageHandler()`, `handlePipelineMessage()`

3. **Message Transport**: Stream-based transport using morpc
   - Message type: `pipeline.Message`
   - Transport protocol: RPC Stream

## Client Implementation (Sender)

### 1. RemoteRun Entry Point

**File:** `pkg/sql/compile/scope.go:385-435`

```go
func (s *Scope) RemoteRun(c *Compile) error {
    // Initialize scope analyzer
    if s.ScopeAnalyzer == nil {
        s.ScopeAnalyzer = NewScopeAnalyzer()
    }
    s.ScopeAnalyzer.Start()
    defer s.ScopeAnalyzer.Stop()
    
    // Check if should execute locally (same address)
    if s.ipAddrMatch(c.addr) {
        return s.MergeRun(c)  // Local execution
    }
    
    // Check if any operator cannot be executed remotely
    if err := s.holdAnyCannotRemoteOperator(); err != nil {
        return err
    }
    
    // Check if pipeline can be executed standalone at remote
    if !checkPipelineStandaloneExecutableAtRemote(s) {
        return s.MergeRun(c)  // Fallback to local execution
    }
    
    // Create pipeline and send to remote node
    p := pipeline.New(0, nil, s.RootOp)
    sender, err := s.remoteRun(c)
    
    runErr := err
    if s.Proc.Ctx.Err() != nil {
        runErr = nil
    }
    // Cleanup using CleanRootOperator (not general cleanup)
    p.CleanRootOperator(s.Proc, err != nil, c.isPrepare, err)
    
    // Close sender after cleanup
    if sender != nil {
        sender.close()
    }
    return runErr
}
```

**Key Logic:**
- Initialize and start scope analyzer for metrics
- Check if same address (execute locally via `MergeRun`)
- Check if any operator cannot be executed remotely via `holdAnyCannotRemoteOperator()`
- Check if pipeline can be executed standalone at remote via `checkPipelineStandaloneExecutableAtRemote`
- If not standalone executable, fallback to local execution (`MergeRun`)
- Create pipeline and call `remoteRun` to send
- Use `CleanRootOperator` for cleanup (not general cleanup)

### 2. remoteRun Implementation

**File:** `pkg/sql/compile/remoterunClient.go:52-102`

```go
func (s *Scope) remoteRun(c *Compile) (sender *messageSenderOnClient, err error) {
    // Encode scope and process information
    scopeEncodeData, withoutOutput, processEncodeData, err := 
        prepareRemoteRunSendingData(c.sql, s)
    
    // Create message sender
    sender, err = newMessageSenderOnClient(
        s.Proc.Ctx,
        s.Proc.GetService(),
        s.NodeInfo.Addr,
        s.Proc.Mp(),
        c.anal,
    )
    
    // Send pipeline message
    err = sender.sendPipeline(scopeEncodeData, processEncodeData, 
        withoutOutput, maxMessageSizeToMoRpc, debugMsg)
    
    // Receive remote execution results
    err = receiveMessageFromCnServer(s, withoutOutput, sender)
    return sender, err
}
```

**Key Steps:**
1. **Encode Data**: Serialize scope and process information to byte arrays
2. **Create Connection**: Establish RPC stream connection to remote CN
3. **Send Message**: Send pipeline message (may be fragmented)
4. **Receive Results**: Receive execution results (batch data or error information)

### 3. prepareRemoteRunSendingData

**File:** `pkg/sql/compile/remoterunClient.go:174-209`

```go
func prepareRemoteRunSendingData(sqlStr string, s *Scope) (
    scopeData []byte, withoutOutput bool, processData []byte, err error) {
    
    // Handle Connector or Dispatch operator (need to keep locally)
    if lastOpType := s.RootOp.OpType(); lastOpType == vm.Connector || 
        lastOpType == vm.Dispatch {
        withoutOutput = false
        // Remove last operator as it needs to be handled locally
        // ...
    }
    
    // Encode Scope
    scopeData, err = encodeScope(s)
    
    // Encode Process information
    processData, err = encodeProcessInfo(s.Proc, sqlStr)
    
    return scopeData, withoutOutput, processData, nil
}
```

**Key Logic:**
- If the last operator is `Connector` or `Dispatch`, it needs to be kept locally
- Encode scope tree structure
- Encode process information (including txnOperator, sessionInfo, etc.)

## Server Implementation (Receiver)

### 1. CnServerMessageHandler Entry Point

**File:** `pkg/sql/compile/remoterunServer.go:67-126`

```go
func CnServerMessageHandler(
    ctx context.Context,
    serverAddress string,
    message morpc.Message,
    cs morpc.ClientSession,
    storageEngine engine.Engine,
    fileService fileservice.FileService,
    lockService lockservice.LockService,
    queryClient qclient.QueryClient,
    HaKeeper logservice.CNHAKeeperClient,
    udfService udf.Service,
    txnClient client.TxnClient,
    autoIncreaseCM *defines.AutoIncrCacheManager,
    messageAcquirer func() morpc.Message) (err error) {
    
    // Validate message type
    msg, ok := message.(*pipeline.Message)
    
    // Create message receiver
    receiver := newMessageReceiverOnServer(ctx, serverAddress, msg,
        cs, messageAcquirer, storageEngine, fileService, lockService,
        queryClient, HaKeeper, udfService, txnClient, autoIncreaseCM)
    
    // Handle message
    err = handlePipelineMessage(&receiver)
    
    // Send response (error or end message)
    if receiver.messageTyp != pipeline.Method_StopSending {
        if err != nil {
            err = receiver.sendError(err)
        } else {
            err = receiver.sendEndMessage()
        }
    }
    
    return err
}
```

**Key Steps:**
1. Validate message type
2. Create message receiver
3. Handle pipeline message
4. Send response

### 2. handlePipelineMessage

**File:** `pkg/sql/compile/remoterunServer.go:128-233`

```go
func handlePipelineMessage(receiver *messageReceiverOnServer) error {
    switch receiver.messageTyp {
    case pipeline.Method_PrepareDoneNotifyMessage:
        // Handle dispatch operator notification message
        // ...
        
    case pipeline.Method_PipelineMessage:
        // Create compile object
        runCompile, errBuildCompile := receiver.newCompile()
        
        // Decode scope
        s, err := decodeScope(receiver.scopeData, runCompile.proc, true, runCompile.e)
        
        // Add write-back operator (if need to return results)
        if !receiver.needNotReply {
            s = appendWriteBackOperator(runCompile, s)
        }
        
        // Create workspace early before any code that might access it
        if runCompile.proc.GetTxnOperator() != nil && 
            runCompile.proc.GetTxnOperator().GetWorkspace() == nil {
            if disttaeEngine, ok := runCompile.e.(*disttae.Engine); ok {
                ws := disttae.NewTxnWorkSpace(disttaeEngine, runCompile.proc)
                runCompile.proc.GetTxnOperator().AddWorkspace(ws)
                ws.BindTxnOp(runCompile.proc.GetTxnOperator())
            }
        }
        
        // Initialize pipeline context
        runCompile.scopes = []*Scope{s}
        runCompile.InitPipelineContextToExecuteQuery()
        
        // Execute pipeline
        err = s.MergeRun(runCompile)
        
        // Generate physical plan
        if err == nil {
            runCompile.GenPhyPlan(runCompile)
            receiver.phyPlan = runCompile.anal.GetPhyPlan()
        }
        
        return err
        
    case pipeline.Method_StopSending:
        // Stop sending message
        colexec.Get().CancelPipelineSending(receiver.clientSession, receiver.messageId)
    
    default:
        panic(fmt.Sprintf("unknown pipeline message type %d.", receiver.messageTyp))
    }
    return nil
}
```

**Key Steps:**
1. **Create Compile**: Rebuild compile object from serialized process information
2. **Decode Scope**: Decode byte array to scope tree structure
3. **Create Workspace**: Create workspace early to ensure it's available before any access
4. **Initialize Context**: Set up pipeline execution context
5. **Execute Pipeline**: Call `MergeRun` to execute pipeline
6. **Generate Plan**: Generate physical execution plan for analysis

### 3. newCompile Implementation

**File:** `pkg/sql/compile/remoterunServer.go:371-421`

```go
func (receiver *messageReceiverOnServer) newCompile() (*Compile, error) {
    // Create memory pool
    mp, err := mpool.NewMPool("compile", 0, mpool.NoFixed)
    
    // Rebuild process from serialized process information
    pHelper, cnInfo := receiver.procBuildHelper, receiver.cnInformation
    runningCtx := defines.AttachAccountId(receiver.messageCtx, pHelper.accountId)
    
    proc := process.NewTopProcess(
        runningCtx,
        mp,
        pHelper.txnClient,
        pHelper.txnOperator,  // Rebuilt txnOperator from serialized data
        cnInfo.fileService,
        cnInfo.lockService,
        cnInfo.queryClient,
        cnInfo.hakeeper,
        cnInfo.udfService,
        cnInfo.aicm,
        nil)
    
    // Set process attributes
    proc.Base.UnixTime = pHelper.unixTime
    proc.Base.Id = pHelper.id
    proc.Base.Lim = pHelper.lim
    proc.Base.SessionInfo = pHelper.sessionInfo
    proc.Base.SessionInfo.StorageEngine = cnInfo.storeEngine
    proc.SetPrepareParams(pHelper.prepareParams)
    
    // Create StmtProfile
    txn := proc.GetTxnOperator().Txn()
    txnId := txn.GetID()
    proc.Base.StmtProfile = process.NewStmtProfile(uuid.UUID(txnId), pHelper.StmtId)
    
    // Create Compile object
    c := allocateNewCompile(proc)
    c.execType = planner.ExecTypeAP_MULTICN
    c.e = cnInfo.storeEngine
    c.MessageBoard = c.MessageBoard.SetMultiCN(c.GetMessageCenter(), 
        c.proc.GetStmtProfile().GetStmtId())
    c.proc.SetMessageBoard(c.MessageBoard)
    c.anal = newAnalyzeModule()
    c.addr = receiver.cnInformation.cnAddr
    
    // Set result callback function
    c.fill = func(b *batch.Batch, counter *perfcounter.CounterSet) error {
        return receiver.sendBatch(b)
    }
    
    return c, nil
}
```

**Key Points:**
- `txnOperator` is rebuilt from serialized snapshot information (`cli.NewWithSnapshot`)
- The rebuilt `txnOperator` has `nil` workspace initially
- Workspace needs to be created manually and bound to `txnOperator`

### 4. decodeScope Implementation

**File:** `pkg/sql/compile/remoterun.go:99-123`

```go
func decodeScope(data []byte, proc *process.Process, isRemote bool, 
    eng engine.Engine) (*Scope, error) {
    
    // Deserialize pipeline
    p := &pipeline.Pipeline{}
    err := p.Unmarshal(data)
    
    // Create scope context
    ctx := &scopeContext{
        parent: nil,
        id:     p.PipelineId,
        regs:   make(map[*process.WaitRegister]int32),
    }
    ctx.root = ctx
    
    // Generate scope tree
    s, err := generateScope(proc, p, ctx, isRemote)
    
    // Fill instructions (converts pipeline instructions to vm operators)
    if err = fillInstructionsForScope(s, ctx, p, eng); err != nil {
        s.release()
        return nil, err
    }
    
    return s, nil
}
```

**Key Points:**
- `fillInstructionsForScope` calls `convertToVmOperator` to convert pipeline instructions to VM operators
- Note: `fillInstructionsForScope` does NOT call operator's `Prepare` method - Prepare is called later during pipeline execution in `MergeRun`
- However, workspace should still be created early because `InitPipelineContextToExecuteQuery` (called after decodeScope) may access workspace

### 5. generateProcessHelper

**File:** `pkg/sql/compile/remoterunServer.go:501-544`

```go
func generateProcessHelper(data []byte, cli client.TxnClient) (processHelper, error) {
    // Deserialize ProcessInfo
    procInfo := &pipeline.ProcessInfo{}
    err := procInfo.Unmarshal(data)
    
    result := processHelper{
        id:        procInfo.Id,
        lim:       process.ConvertToProcessLimitation(procInfo.Lim),
        unixTime:  procInfo.UnixTime,
        accountId: procInfo.AccountId,
        txnClient: cli,
    }
    
    // Handle PrepareParams
    if procInfo.PrepareParams.Length > 0 {
        result.prepareParams = vector.NewVecWithData(...)
    }
    
    // Rebuild txnOperator from snapshot
    result.txnOperator, err = cli.NewWithSnapshot(procInfo.Snapshot)
    
    // Convert SessionInfo
    result.sessionInfo, err = process.ConvertToProcessSessionInfo(procInfo.SessionInfo)
    
    return result, nil
}
```

**Key Points:**
- `NewWithSnapshot` creates a `txnOperator` with `nil` workspace
- Workspace needs to be created manually afterwards

## Message Flow

### Client to Server

```
Client CN                          Server CN
   |                                   |
   |-- encodeScope()                   |
   |-- encodeProcessInfo()             |
   |-- sendPipeline() ----------------->|
   |                                   |-- CnServerMessageHandler()
   |                                   |-- handlePipelineMessage()
   |                                   |-- newCompile()
   |                                   |-- decodeScope()
   |                                   |-- Create workspace
   |                                   |-- MergeRun()
   |                                   |-- Execute pipeline
   |<-- receiveBatch() ----------------|
   |                                   |
```

### Message Types

1. **PipelineMessage**: Contains pipeline execution request
   - Scope data (encoded scope tree)
   - Process data (encoded process information)
   - Whether need to return results (`NeedNotReply`)

2. **BatchMessage**: Contains execution result data
   - Batch data (may be fragmented)
   - Status flags (`Status_Last`, `Status_WaitingNext`)

3. **EndMessage**: Execution end message
   - Error information (if any)
   - Analysis information (physical plan)

4. **StopSending**: Stop sending message

## Key Data Structures

### messageReceiverOnServer

**File:** `pkg/sql/compile/remoterunServer.go:281-302`

```go
type messageReceiverOnServer struct {
    messageCtx    context.Context
    connectionCtx context.Context
    
    messageId   uint64
    messageTyp  pipeline.Method
    messageUuid uuid.UUID
    
    cnInformation cnInformation  // CN node information
    procBuildHelper processHelper // Process build helper information
    
    clientSession   morpc.ClientSession
    messageAcquirer func() morpc.Message
    maxMessageSize  int
    scopeData       []byte  // Encoded scope data
    
    needNotReply bool
    
    phyPlan *models.PhyPlan  // Physical plan (result)
}
```

### messageSenderOnClient

**File:** `pkg/sql/compile/remoterunClient.go:318-344`

```go
type messageSenderOnClient struct {
    ctx       context.Context
    ctxCancel context.CancelFunc
    
    mp *mpool.MPool
    
    anal *AnalyzeModule
    
    streamSender morpc.Stream
    receiveCh    chan morpc.Message
    
    safeToClose bool
    alreadyClose bool
}
```

## Execution Flow

### Complete Flow

```
1. Client: Scope.RemoteRun()
   └─ Check if can execute remotely
   └─ Scope.remoteRun()
       ├─ prepareRemoteRunSendingData()  // Encode scope and process
       ├─ newMessageSenderOnClient()     // Create RPC connection
       └─ sender.sendPipeline()          // Send pipeline message

2. Server: CnServerMessageHandler()
   └─ handlePipelineMessage()
       └─ case Method_PipelineMessage:
           ├─ newCompile()               // Rebuild compile object
           │   └─ generateProcessHelper() // Rebuild txnOperator from snapshot
           ├─ decodeScope()              // Decode scope
           │   └─ fillInstructionsForScope() // Fill instructions (may call Prepare)
           ├─ Create workspace           // Create workspace early
           ├─ InitPipelineContextToExecuteQuery() // Initialize context
           └─ MergeRun()                 // Execute pipeline
               └─ getRelData()
                   └─ handleDbRelContext() // Check workspace (avoid duplicate creation)

3. Server: Return execution results
   └─ receiver.sendBatch()  // Send batch data
   └─ receiver.sendEndMessage() // Send end message

4. Client: Receive results
   └─ receiveMessageFromCnServer()
       └─ sender.receiveBatch() // Receive batch data
```

## Workspace Management

### Workspace Creation

In remote run scenarios, workspace is created early in the server-side message handler to ensure it's available before any code that might access it:

**File:** `pkg/sql/compile/remoterunServer.go:192-202`

```go
// Create workspace early before any code that might access it
if runCompile.proc.GetTxnOperator() != nil && 
    runCompile.proc.GetTxnOperator().GetWorkspace() == nil {
    if disttaeEngine, ok := runCompile.e.(*disttae.Engine); ok {
        ws := disttae.NewTxnWorkSpace(disttaeEngine, runCompile.proc)
        runCompile.proc.GetTxnOperator().AddWorkspace(ws)
        ws.BindTxnOp(runCompile.proc.GetTxnOperator())
    }
}
```

### Workspace Check in handleDbRelContext

To avoid duplicate creation, `handleDbRelContext` checks if workspace already exists:

**File:** `pkg/sql/compile/compile.go:4222-4236`

```go
func (c *Compile) handleDbRelContext(node *plan.Node, onRemoteCN bool) (engine.Relation, engine.Database, context.Context, error) {
    var err error
    var db engine.Database
    var rel engine.Relation
    var txnOp client.TxnOperator

    if onRemoteCN {
        // Workspace may have been created earlier in remote run scenario.
        // Only create if it doesn't exist to avoid duplicate creation.
        if c.proc.GetTxnOperator().GetWorkspace() == nil {
            ws := disttae.NewTxnWorkSpace(c.e.(*disttae.Engine), c.proc)
            c.proc.GetTxnOperator().AddWorkspace(ws)
            ws.BindTxnOp(c.proc.GetTxnOperator())
        }
    }
    // ... rest of function
}
```

## Related Code Locations

- `pkg/sql/compile/remoterunServer.go` - Server-side implementation
- `pkg/sql/compile/remoterunClient.go` - Client-side implementation
- `pkg/sql/compile/remoterun.go` - Encoding/decoding utilities
- `pkg/sql/compile/scope.go:385-435` - RemoteRun entry point
- `pkg/sql/compile/compile.go:4222-4236` - handleDbRelContext function
- `pkg/txn/client/operator.go:456-462` - AddWorkspace and GetWorkspace methods
