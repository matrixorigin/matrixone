package main

import (
	"context"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/util/export"
	"net/http"
	"os"
	"runtime/pprof"
	"sync"
	"time"

	_ "net/http/pprof"
)

var (
	etlFileServiceName = "etl"
	statsDatabase      = "system"
	statementInfoTbl   = "statement_info"
	rawLogTbl          = "rawlog"

	uuidColType        = "varchar(36)"
	spanIDType         = "varchar(16)"
	datetime6Type      = "datetime(6)"
	bigintUnsignedType = "bigint unsigned"
	stringType         = "varchar(1024)"
	jsonColumnDEFAULT  = "{}"

	stmtIDCol    = export.Column{Name: "statement_id", Type: uuidColType, Default: "0", Comment: "statement uniq id"}
	txnIDCol     = export.Column{Name: "transaction_id", Type: uuidColType, Default: "0", Comment: "txn uniq id"}
	sesIDCol     = export.Column{Name: "session_id", Type: uuidColType, Default: "0", Comment: "session uniq id"}
	accountCol   = export.Column{Name: "account", Type: stringType, Default: "", Comment: "account name"}
	userCol      = export.Column{Name: "user", Type: stringType, Default: "", Comment: "user name"}
	hostCol      = export.Column{Name: "host", Type: stringType, Default: "", Comment: "user client ip"}
	dbCol        = export.Column{Name: "database", Type: stringType, Default: "", Comment: "what database current session stay in."}
	stmtCol      = export.Column{Name: "statement", Type: "TEXT", Default: "", Comment: "sql statement"}
	stmtTagCol   = export.Column{Name: "statement_tag", Type: "TEXT", Default: "", Comment: "note tag in statement(Reserved)"}
	stmtFgCol    = export.Column{Name: "statement_fingerprint", Type: "TEXT", Default: "", Comment: "note tag in statement(Reserved)"}
	nodeUUIDCol  = export.Column{Name: "node_uuid", Type: uuidColType, Default: "0", Comment: "node uuid, which node gen this data."}
	nodeTypeCol  = export.Column{Name: "node_type", Type: "varchar(64)", Default: "node", Comment: "node type in MO, val in [DN, CN, LOG]"}
	reqAtCol     = export.Column{Name: "request_at", Type: datetime6Type, Default: "", Comment: "request accept datetime"}
	respAtCol    = export.Column{Name: "response_at", Type: datetime6Type, Default: "", Comment: "response send datetime"}
	durationCol  = export.Column{Name: "duration", Type: bigintUnsignedType, Default: "0", Comment: "exec time, unit: ns"}
	statusCol    = export.Column{Name: "status", Type: "varchar(32)", Default: "Running", Comment: "sql statement running status, enum: Running, Success, Failed"}
	errorCol     = export.Column{Name: "error", Type: "TEXT", Default: "", Comment: "error message"}
	execPlanCol  = export.Column{Name: "exec_plan", Type: "JSON", Default: jsonColumnDEFAULT, Comment: "statement execution plan"}
	rowsReadCol  = export.Column{Name: "rows_read", Type: bigintUnsignedType, Default: "0", Comment: "rows read total"}
	bytesScanCol = export.Column{Name: "bytes_scan", Type: bigintUnsignedType, Default: "0", Comment: "bytes scan total"}

	dummyStatementTable = &export.Table{
		Account:  export.AccountAll,
		Database: statsDatabase,
		Table:    statementInfoTbl,
		Columns: []export.Column{
			stmtIDCol,
			txnIDCol,
			sesIDCol,
			accountCol,
			userCol,
			hostCol,
			dbCol,
			stmtCol,
			stmtTagCol,
			stmtFgCol,
			nodeUUIDCol,
			nodeTypeCol,
			reqAtCol,
			respAtCol,
			durationCol,
			statusCol,
			errCodeCol,
			errorCol,
			execPlanCol,
			rowsReadCol,
			bytesScanCol,
		},
		PrimaryKeyColumn: []export.Column{stmtIDCol},
		Engine:           export.ExternalTableEngine,
		Comment:          "record each statement and stats info",
		PathBuilder:      export.NewAccountDatePathBuilder(),
		AccountColumn:    &accountCol,
		// SupportUserAccess
		SupportUserAccess: true,
	}

	rawItemCol      = export.Column{Name: "raw_item", Type: stringType, Comment: "raw log item"}
	timestampCol    = export.Column{Name: "timestamp", Type: datetime6Type, Comment: "timestamp of action"}
	loggerNameCol   = export.Column{Name: "logger_name", Type: stringType, Comment: "logger name"}
	levelCol        = export.Column{Name: "level", Type: stringType, Comment: "log level, enum: debug, info, warn, error, panic, fatal"}
	callerCol       = export.Column{Name: "caller", Type: stringType, Comment: "where it log, like: package/file.go:123"}
	messageCol      = export.Column{Name: "message", Type: "TEXT", Comment: "log message"}
	extraCol        = export.Column{Name: "extra", Type: "JSON", Default: jsonColumnDEFAULT, Comment: "log dynamic fields"}
	errCodeCol      = export.Column{Name: "err_code", Type: stringType, Default: "0"}
	stackCol        = export.Column{Name: "stack", Type: "varchar(4096)"}
	spanIDCol       = export.Column{Name: "span_id", Type: spanIDType, Default: "0", Comment: "span uniq id"}
	parentSpanIDCol = export.Column{Name: "parent_span_id", Type: spanIDType, Default: "0", Comment: "parent span uniq id"}
	spanNameCol     = export.Column{Name: "span_name", Type: stringType, Default: "", Comment: "span name, for example: step name of execution plan, function name in code, ..."}
	startTimeCol    = export.Column{Name: "start_time", Type: datetime6Type, Default: ""}
	endTimeCol      = export.Column{Name: "end_time", Type: datetime6Type, Default: ""}
	resourceCol     = export.Column{Name: "resource", Type: "JSON", Default: jsonColumnDEFAULT, Comment: "static resource information"}

	dummyRawlogTable = &export.Table{
		Account:  export.AccountAll,
		Database: statsDatabase,
		Table:    rawLogTbl,
		Columns: []export.Column{
			rawItemCol,
			nodeUUIDCol,
			nodeTypeCol,
			spanIDCol,
			stmtIDCol,
			loggerNameCol,
			timestampCol,
			levelCol,
			callerCol,
			messageCol,
			extraCol,
			errCodeCol,
			errorCol,
			stackCol,
			spanNameCol,
			parentSpanIDCol,
			startTimeCol,
			endTimeCol,
			durationCol,
			resourceCol,
		},
		PrimaryKeyColumn: nil,
		Engine:           export.ExternalTableEngine,
		Comment:          "read merge data from log, error, span",
		PathBuilder:      export.NewAccountDatePathBuilder(),
		AccountColumn:    nil,
		// SupportUserAccess
		SupportUserAccess: false,
	}
)

func main() {

	ctx := context.Background()

	fs, err := fileservice.NewLocalETLFS(etlFileServiceName, "mo-data/etl")
	if err != nil {
		logutil.Infof("failed open fileservice: %v\n", err)
		return
	}
	files, err := fs.List(ctx, "/")
	if err != nil {
		logutil.Infof("failed list /: %v\n", err)
		return
	}
	if len(files) == 0 {
		logutil.Infof("skipping, no mo-data/etl folder")
		return
	}

	httpWG := sync.WaitGroup{}
	httpWG.Add(1)
	go func() {
		httpWG.Done()
		http.ListenAndServe("0.0.0.0:8123", nil)
	}()
	httpWG.Wait()
	time.Sleep(time.Second)

	//merge := export.NewMerge(ctx, export.WithTable(dummyStatementTable), export.WithFileService(fs))
	merge := export.NewMerge(ctx, export.WithTable(dummyRawlogTable), export.WithFileService(fs))
	logutil.Infof("[%v] create merge task\n", time.Now())
	ts, err := time.Parse("2006-01-02 15:04:05", "2022-11-03 00:00:00")
	logutil.Infof("[%v] create ts: %v, err: %v\n", time.Now(), ts, err)
	err = merge.Main(ts)
	if err != nil {
		logutil.Infof("[%v] failed to merge: %v\n", time.Now(), err)
	} else {
		logutil.Infof("[%v] merge succeed.", time.Now())
	}

	writeAllocsProfile()

}

func writeAllocsProfile() {
	profile := pprof.Lookup("heap")
	if profile == nil {
		return
	}
	profilePath := ""
	if profilePath == "" {
		profilePath = "heap-profile"
	}
	f, err := os.Create(profilePath)
	if err != nil {
		panic(err)
	}
	defer f.Close()
	if err := profile.WriteTo(f, 0); err != nil {
		panic(err)
	}
	logutil.Infof("Allocs profile written to %s", profilePath)
}
