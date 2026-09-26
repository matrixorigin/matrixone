import java.sql.*;
import java.util.Objects;

// Connector/J 8.4.0; see README.md. No emulated prepared statements are allowed.
public final class TemporalUpgradeProbe {
    static void check(boolean ok, String message) {
        if (!ok) throw new AssertionError(message);
    }
    static String scalar(Statement s, String sql) throws Exception {
        try (ResultSet r = s.executeQuery(sql)) {
            check(r.next(), sql);
            return r.getString(1);
        }
    }
    static PreparedStatement prepare(Connection c, String sql) throws Exception {
        PreparedStatement p = c.prepareStatement(sql);
        check(p instanceof com.mysql.cj.jdbc.ServerPreparedStatement, "emulated prepare: " + sql);
        return p;
    }
    static void warnings(Statement s, int count, int code) throws Exception {
        int seen = 0;
        try (ResultSet r = s.executeQuery("show warnings")) {
            while (r.next()) { ++seen; check(r.getInt(2) == code, "warning code " + r.getInt(2)); }
        }
        check(seen == count, "warning count expected=" + count + " actual=" + seen);
    }
    static void arithmeticContract(Connection c, Statement s) throws Exception {
        s.execute("set sql_mode=''");
        s.execute("set time_zone='+00:00'");
        // Independent values, result family, scale and diagnostics on both wires.
        Object[][] cases = {
            {"addtime(time '838:59:59','00:00:01')", null, Types.TIME, 0, 1},
            {"subtime(time '-838:59:59','00:00:01')", null, Types.TIME, 0, 1},
            {"timediff('838:59:59','-00:00:01')", null, Types.TIME, 0, 1},
            {"subtime(cast('0001-01-01' as datetime(6)),'00:00:01')", null, Types.TIMESTAMP, 6, 1},
            {"addtime(cast('9999-12-31 23:59:59.999999' as datetime(6)),'00:00:00.000001')", null, Types.TIMESTAMP, 6, 1},
            {"date_sub(cast('1970-01-01 00:00:01' as timestamp(6)),interval 1 microsecond)", null, Types.TIMESTAMP, 6, 1},
            {"date_sub(cast('0001-01-01' as date),interval 1 day)", null, Types.DATE, 0, 1},
            {"date_add(cast('2024-01-01' as datetime),interval '9999999999999999999999999999' second)", null, Types.TIMESTAMP, 0, 1},
            {"date_add(cast(null as datetime),interval '9999999999999999999999999999' second)", null, Types.TIMESTAMP, 0, 0},
            {"date_add(cast('2024-01-01' as datetime),interval 'bad' hour_second)", null, Types.TIMESTAMP, 0, 0},
            {"date_add(time '12:00:00',interval '' hour_second)", null, Types.TIME, 6, 0},
            {"maketime(839,0,0)", null, Types.TIME, 0, 1},
            {"addtime('1234:00:00','-500:00:00')", "734:00:00", Types.VARCHAR, 6, 0},
            {"timestamp('2024-01-01','839:00:00')", "2024-02-04 23:00:00", Types.TIMESTAMP, 0, 0},
            {"addtime(time '12:00:00','00:00:00.1')", "12:00:00.1", Types.TIME, 1, 0},
            {"subtime(cast('2024-01-01 12:00:00' as datetime),'00:00:00.1')", "2024-01-01 11:59:59.9", Types.TIMESTAMP, 1, 0},
            {"cast('50-01-01 00:00:01' as datetime(6))", "2050-01-01 00:00:01", Types.TIMESTAMP, 6, 0},
            {"cast('90-01-01 00:00:01' as datetime(6))", "1990-01-01 00:00:01", Types.TIMESTAMP, 6, 0},
            {"cast('24.2.29 1:2:3.4' as datetime(6))", "2024-02-29 01:02:03.4", Types.TIMESTAMP, 6, 0},
            {"extract(year from '2024-00-15')", "2024", Types.BIGINT, 0, 0},
            {"extract(day from '2024-00-15')", "15", Types.BIGINT, 0, 0},
            {"extract(week from '0000-00-00')", null, Types.BIGINT, 0, 0},
            {"extract(hour from '')", null, Types.BIGINT, 0, 0},
            {"case when 0 then addtime(time '838:59:59','00:00:01') else time '00:00:01' end", "00:00:01", Types.TIME, 0, 0},
            {"case when 0 then timediff('900:00:00','00:00:00') else time '00:00:01' end", "00:00:01", Types.TIME, 0, 0},
            {"case when 0 then date_add(time '838:59:59',interval 1 second) else time '00:00:01' end", "00:00:01", Types.TIME, 0, 0}
        };
        for (boolean binary : new boolean[]{false, true}) {
            for (Object[] test : cases) {
                String sql = "select " + test[0];
                try (Statement q = binary ? prepare(c, sql) : c.createStatement()) {
                    for (int repeat = 0; repeat < (binary ? 2 : 1); ++repeat) {
                        try (ResultSet r = binary ? ((PreparedStatement)q).executeQuery() : q.executeQuery(sql)) {
                            ResultSetMetaData m = r.getMetaData();
                            check(m.getColumnType(1) == (int)test[2], "type: " + sql + " got " + m.getColumnType(1));
                            // JDBC getScale() reports 0 for temporal types in Connector/J;
                            // inspect the actual MySQL field decimals instead.
                            com.mysql.cj.result.Field field = ((com.mysql.cj.jdbc.result.ResultSetImpl)r).getMetadata().getFields()[0];
                            if ((int)test[2] != Types.VARCHAR)
                                check(field.getDecimals() == (int)test[3], "wire decimals: " + sql + " got " + field.getDecimals());
                            else check(field.getLength() == 29 * 4, "VARCHAR(29) width: " + field.getLength());
                            check(r.next(), "missing row: " + sql);
                            if ((int)test[2] == Types.TIMESTAMP && test[1] != null) {
                                java.time.LocalDateTime want = java.time.LocalDateTime.parse(((String)test[1]).replace(' ', 'T'));
                                check(want.equals(r.getObject(1, java.time.LocalDateTime.class)), "calendar value: " + sql);
                            } else check(Objects.equals(test[1], r.getString(1)), "value: " + sql + " got " + r.getString(1));
                            check(!r.next(), "single row: " + sql);
                        }
                        warnings(s, (int)test[4], 1441);
                    }
                }
            }
        }
        try (PreparedStatement p = prepare(c, "select extract(year from '0000-00-00'),extract(year from ?),extract(hour from ?)");
             PreparedStatement interval = prepare(c, "select date_add(cast(? as datetime), interval ? hour_second)")) {
            for (String mode : new String[]{"", "NO_ZERO_DATE", ""}) {
                s.execute("set sql_mode='" + mode + "'");
                p.setString(1,"0000-00-00"); p.setString(2,"0000-00-00 12:34:56");
                try (ResultSet r = p.executeQuery()) {
                    check(r.next(), "mode row");
                    String want = mode.isEmpty() ? "0" : null;
                    check(Objects.equals(want, r.getString(1)) && Objects.equals(want, r.getString(2)), "execute-time EXTRACT mode");
                    check(r.getLong(3) == 12, "zero calendar preserves clock");
                }
            }
            for (boolean bytes : new boolean[]{false, true}) {
                for (String text : new String[]{"1:02:03", "bad", "", null, "9999999999999999999999999999", "0", "1:02:03"}) {
                    interval.setString(1,"2024-01-01");
                    if (text == null) interval.setNull(2, Types.VARCHAR);
                    else if (bytes) interval.setBytes(2,text.getBytes(java.nio.charset.StandardCharsets.US_ASCII));
                    else interval.setString(2,text);
                    try (ResultSet r = interval.executeQuery()) {
                        check(r.next(), "rebind row");
                        String want = "1:02:03".equals(text) ? "2024-01-01 01:02:03" : "0".equals(text) ? "2024-01-01 00:00:00" : null;
                        check(Objects.equals(want,r.getString(1)), "interval rebind: " + text + " got " + r.getString(1));
                    }
                    warnings(s, text != null && text.startsWith("999") ? 1 : 0, 1441);
                }
            }
        }
        System.out.println("PASS temporal contract: exact bounds, inactive diagnostics, text/binary metadata, mode and ASCII rebinding");
    }
    public static void main(String[] args) throws Exception {
        String url = System.getenv("MO_JDBC_URL");
        check(url != null, "MO_JDBC_URL is required");
        url += (url.contains("?") ? "&" : "?")
            + "useServerPrepStmts=true&emulateUnsupportedPstmts=false&cachePrepStmts=false";
        try (Connection c = DriverManager.getConnection(url, System.getenv("MO_USER"),
                System.getenv("MO_PASSWORD")); Statement s = c.createStatement()) {
            if (args.length == 1 && args[0].equals("contract")) { arithmeticContract(c, s); return; }
            if (args.length == 1 && args[0].equals("seed")) {
                check(scalar(s, "select version()").contains("4.2."), "seed must run on a 4.2 release");
                s.execute("create database qa_temporal_upgrade_28851");
                s.execute("use qa_temporal_upgrade_28851");
                s.execute("create table persisted(id int primary key, "
                    + "ex varchar(20) default(extract(minute from current_timestamp)), "
                    + "ex_date int default(extract(month from current_date)), "
                    + "a datetime default(addtime(date_format(current_timestamp,'%Y-%m-%d 01:00:00'),'01:00:00')), "
                    + "b datetime default(subtime(date_format(current_timestamp,'%Y-%m-%d 01:00:00'),'01:00:00'))) ");
                s.execute("insert into persisted(id) values(1)");
                s.execute("create view rebound as select id, extract(hour from a) as h from persisted");
                check("1".equals(scalar(s, "select count(*) from persisted where length(ex)=2 and ex_date between 1 and 12 and hour(a)=2 and hour(b)=0")), "release defaults");
                System.out.println("PASS 4.2 seed: defaults and view persisted");
                return;
            }
            check("2".equals(scalar(s, "select count(*) from mo_catalog.mo_tables where account_id=0 "
                + "and reldatabase='mo_catalog' and relname in ('mo_view_dependencies','mo_view_refresh')")),
                "release upgrade must create both view metadata tables before restart admission");
            s.execute("use qa_temporal_upgrade_28851");
            s.execute("insert into persisted(id) values(2)");
            check("2".equals(scalar(s, "select count(*) from persisted where length(ex)=2 and ex_date between 1 and 12 and hour(a)=2 and hour(b)=0")), "upgraded defaults");
            for (boolean binary : new boolean[]{false, true}) {
                try (Statement q = binary ? prepare(c, "select h from rebound order by id") : c.createStatement();
                     ResultSet r = binary ? ((PreparedStatement)q).executeQuery() : q.executeQuery("select h from rebound order by id")) {
                    check(r.getMetaData().getColumnType(1) == Types.BIGINT, "rebound EXTRACT metadata");
                    check(r.next() && r.getLong(1) == 2, "existing row");
                    check(r.next() && r.getLong(1) == 2 && !r.next(), "new row");
                }
            }
            s.execute("create table payload(id int primary key,t time null)");
            try (PreparedStatement insert = prepare(c, "insert into payload values (?,?)");
                 PreparedStatement update = prepare(c, "update payload set t=? where id=?");
                 PreparedStatement cast = prepare(c, "select cast(? as time)")) {
                for (String mode : new String[]{"", "STRICT_TRANS_TABLES"}) {
                    s.execute("set sql_mode='" + mode + "'");
                    int id = 0;
                    for (boolean bytes : new boolean[]{false, true}) {
                        // Same handles: valid -> empty -> NULL -> whitespace -> valid.
                        for (String v : new String[]{"15", "", null, " ", "1"}) {
                            String want = v == null || v.isEmpty() ? null
                                : v.equals(" ") ? "00:00:00" : v.equals("15") ? "00:00:15" : "00:00:01";
                            ++id;
                            insert.setInt(1, id);
                            for (PreparedStatement p : new PreparedStatement[]{insert, update, cast}) {
                                int pos = p == insert ? 2 : 1;
                                if (v == null) p.setNull(pos, Types.VARCHAR);
                                else if (bytes) p.setBytes(pos, v.getBytes(java.nio.charset.StandardCharsets.US_ASCII));
                                else p.setString(pos, v);
                            }
                            insert.executeUpdate();
                            check(Objects.equals(want, scalar(s, "select cast(t as char) from payload where id=" + id)), "insert " + mode + " bytes=" + bytes + " value=" + v);
                            s.execute("update payload set t='12:00:00' where id=" + id);
                            update.setInt(2, id);
                            update.executeUpdate();
                            check(Objects.equals(want, scalar(s, "select cast(t as char) from payload where id=" + id)), "update " + mode + " bytes=" + bytes + " value=" + v);
                            try (ResultSet r = cast.executeQuery()) {
                                check(r.getMetaData().getColumnType(1) == Types.TIME, "cast metadata");
                                check(r.next() && Objects.equals(want, r.getString(1)), "cast value");
                            }
                        }
                    }
                    s.execute("delete from payload");
                    s.execute("insert into payload values(100,''),(101,x''),(102,' ')");
                    check(scalar(s,"select cast(t as char) from payload where id=100") == null, "text literal");
                    check("00:00:00".equals(scalar(s,"select cast(t as char) from payload where id=101")), "hex literal");
                    check("00:00:00".equals(scalar(s,"select cast(t as char) from payload where id=102")), "whitespace literal");
                    s.execute("delete from payload");
                }
            }
            arithmeticContract(c, s);
            s.execute("drop table payload");
            s.execute("delete from persisted where id=2");
            System.out.println("PASS 4.2 upgrade: stored/default/view ABI, JDBC text/binary, INSERT/UPDATE/CAST and rebinding");
        }
    }
}
