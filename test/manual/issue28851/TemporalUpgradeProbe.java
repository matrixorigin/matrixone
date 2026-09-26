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
    public static void main(String[] args) throws Exception {
        String url = System.getenv("MO_JDBC_URL");
        check(url != null, "MO_JDBC_URL is required");
        url += (url.contains("?") ? "&" : "?")
            + "useServerPrepStmts=true&emulateUnsupportedPstmts=false&cachePrepStmts=false";
        try (Connection c = DriverManager.getConnection(url, System.getenv("MO_USER"),
                System.getenv("MO_PASSWORD")); Statement s = c.createStatement()) {
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
            s.execute("drop table payload");
            s.execute("delete from persisted where id=2");
            System.out.println("PASS 4.2 upgrade: stored/default/view ABI, JDBC text/binary, INSERT/UPDATE/CAST and rebinding");
        }
    }
}
