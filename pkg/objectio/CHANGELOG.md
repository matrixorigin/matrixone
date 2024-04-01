# Storage Codec Change Log

## MatrixOne 0.8 (2023-06-30)

### Log Entry

| Type | Version | Name                  |
| ---- | ------- | --------------------- |
| 1000 | 1       | Record                |
| 2001 | 1       | Checkpoint            |
| 2002 | 1       | PostCommit            |
| 2003 | 1       | Uncommitted           |
| 2004 | 1       | Txn                   |
| 2005 | 1       | Test                  |
| 2006 | 1       | TxnRecord             |
| 3000 | 1       | TxnEntry              |
| 3001 | 1       | TxnCommand_Composed   |
| 3002 | 1       | TxnCommand_TxnState   |
| 3004 | 1       | TxnCommand_AppendNode |
| 3005 | 1       | TxnCommand_DeleteNode |
| 3006 | 1       | TxnCommand_Compact    |
| 3007 | 1       | TxnCommand_Merge      |
| 3008 | 2       | TxnCommand_Append     |
| 3009 | 1       | TxnCommand_Database   |
| 3010 | 1       | TxnCommand_Table      |
| 3011 | 1       | TxnCommand_Segment    |
| 3012 | 1       | TxnCommand_Block      |

### Data

| Type | Version | Name        |
| ---- | ------- | ----------- |
| 1    | 1       | ObjectMeta  |
| 2    | 1       | ColumnData  |
| 3    | 1       | Bloomfilter |
| 4    | 1       | Zonemap     |

## MatrixOne 1.0

### Log Entry

| Type | Version | Name                              |
| ---- | ------- | --------------------------------- |
| 3005 | 2       | TxnCommand_DeleteNode             |
| 3010 | 3       | TxnCommand_Table                  |
| 3013 | 1       | TxnCommand_PersistedDeleteNode    |
| 3014 | 1       | IOET_WALTxnCommand_FlushTableTail |

## MatrixOne 1.1

### Log Entry
| 3015 | 1       | IOET_WALTxnCommand_Object |
| 3000 | 2       | TxnEntry              |

## MatrixOne 1.2 (TBD)

| Type | Version | Name                              |
| ---- | ------- | --------------------------------- |
| 3004 | 2       | TxnCommand_AppendNode             |
| 3005 | 3       | TxnCommand_DeleteNode             |
| 3013 | 2       | TxnCommand_PersistedDeleteNode    |
| 3009 | 2       | TxnCommand_Database               |
| 3010 | 4       | TxnCommand_Table                  |
| 3015 | 2       | IOET_WALTxnCommand_Object         |