1. Wal
   
     主要功能是：计算checkpoint并通知Driver做truncate，记录checkpointed,synced,lsn。
   * FuzzyCheckpoint
  
      按照group记录所有的checkpoint信息。用closeIntervals记录完全ckp的lsn，用bitmap记录部分ckp的lsn。
   * WalDriverLsnMap
  
      Wal和Driver各自维护一套lsn，Wal的会分group，Driver是全局的。这个map记录wal的lsn和driver的lsn的对应关系。
   * WalLsnTidMap
  
      对每个属于commit group的entry，会记录wal lsn和txnID对应关系
   * DriverCheckpointed
  
      Driver的checkpoint点
   * WalCurrentLsn
  
      每个group分配出去的lsn
   * Synced
  
      每个group刷盘的lsn

2. Driver
   
     主要功能是：控制读写，比如对接logservice就会在这里攒entry。还会记录lsn和Get所需参数的对应关系。

   * lsn和addr（e.g. versionID+offset）的对应关系。
  
##
1. Append
   
   分三个阶段：
   1.  把entry放入队列，返回lsn。
       * Wal.Append():在gid-lsn中alloc并把entry传给Driver和Wal.queue1。 
   2.  entry刷盘。entry.WaitDone结束。
       * Driver.Append(): 确认持久化之后返回, 通知Wal.onQueue1()，传entry的lsn和addr给Driver.queue。
       * Wal.onQueue1():等待Driver返回，执行entry.WaitDone()，传entry给Wal.queue2。
   3.  异步地计算一些信息：各种lsn的对应关系和synced。
       * Wal.onQueue2(): 计算 Synced, WalDriverLsnMap,WalLsnTidMap。
       * Driver.onQueue1(): 计算driver lsn和addr的对应关系。

    ![](image/logstore1.PNG)

2. Checkpoint
   
   1. 把[]*wal.Index打包成checkpoint entry, 传给Driver
      * Wal.Checkpoint()
   1. checkpoint entry持久化。
      * Driver.Append()，和Append流程中类似。
   2. 计算FuzzyCheckpoint，清理checkpoint entry
       *  Wal.CalculateCkp(): 
           1. 计算FuzzyCheckpoint，记录checkpointEntry的DriverLsn。
  
           2. 根据FuzzyCheckpoint和WalDriverLsnMap计算Driver的checkpoint点。
  
           3. 如果最小的pending来自checkpoint entry，就清理checkpoint entry:
  
          *  Wal.CkpCkp()
              1.  生成一份checkpoint的快照
  
              2.  给Driver持久化
  
              3. checkpoint所有快照之前的checkpoint entry
   3. 传checkpoint点给Driver
      * Driver.Truncate

    ![](image/logstore2.PNG)
   