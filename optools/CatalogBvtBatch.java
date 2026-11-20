import io.mo.Tester;
import io.mo.cases.TestScript;
import io.mo.constant.COMMON;
import io.mo.db.Executor;
import io.mo.result.TestReport;
import io.mo.util.RunConfUtil;
import io.mo.util.ScriptParser;
import java.util.Objects;

// mo-tester normally drops databases at every invocation. A routed batch is
// part of one group: initialize on the first batch and finalize on the last.
public final class CatalogBvtBatch {
    public static void main(String[] args) {
        RunConfUtil.setDerivedStaticResourcePath(args[0]);
        if (!args[1].isEmpty()) COMMON.RESOURCE_PATH = args[1];
        COMMON.WAIT_TIMEOUT = RunConfUtil.getWaitTime();
        COMMON.IGNORE_MODEL = true;
        COMMON.IS_COMPARE_META = false;
        COMMON.NEED_PPROF = true;
        Executor executor = new Executor();
        TestReport report = new TestReport();
        if (Boolean.parseBoolean(args[2])) {
            executor.cleanDatabases();
            executor.dropAccountForTest();
            Tester.removeOutfiles();
        }
        for (int i = 4; i < args.length; i++) {
            TestScript script = Objects.requireNonNull(new ScriptParser().parseScript(args[i]), args[i]);
            executor.run(script);
            report.collect(script);
        }
        report.write();
        if (Boolean.parseBoolean(args[3])) Tester.removeOutfiles();
        if (report.getRate() < RunConfUtil.getRate()) System.exit(1);
    }
}
