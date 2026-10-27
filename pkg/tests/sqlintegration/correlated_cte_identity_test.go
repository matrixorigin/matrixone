// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package sqlintegration

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/go-sql-driver/mysql"
	"github.com/matrixorigin/matrixone/pkg/embed"
	"github.com/stretchr/testify/require"
)

// Issue23083 combines the original wide indexed schema with the remaining
// inline-CTE identity boundary. All cases use the canonical shared SQL fixture.
func TestCorrelatedCTEProjectionIdentity(t *testing.T) {
	runSQLIntegration(t, func(c embed.Cluster) {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
		defer cancel()
		cn, err := c.GetCNService(0)
		require.NoError(t, err)
		db, err := sql.Open("mysql", fmt.Sprintf("dump:111@tcp(127.0.0.1:%d)/", cn.GetServiceConfig().CN.Frontend.Port))
		require.NoError(t, err)
		defer db.Close()
		db.SetMaxOpenConns(1)
		defer cleanupSQLIntegration(t, cn, "drop database if exists correlated_cte_identity")
		for _, statement := range []string{"drop database if exists correlated_cte_identity", "create database correlated_cte_identity", "use correlated_cte_identity", `CREATE TABLE company (
  id int NOT NULL,
  full_name varchar(200) DEFAULT NULL,
  short_name varchar(128) DEFAULT NULL,
  update_time datetime NOT NULL DEFAULT current_timestamp() ON UPDATE current_timestamp() COMMENT '最后修改时间',
  address varchar(255) DEFAULT NULL,
  board_id int DEFAULT NULL,
  business_scope text DEFAULT NULL,
  city varchar(32) DEFAULT NULL,
  company_profile text DEFAULT NULL,
  company_type_tags varchar(255) DEFAULT NULL,
  country varchar(20) DEFAULT NULL,
  directory_label varchar(512) DEFAULT NULL,
  district varchar(32) DEFAULT NULL,
  email varchar(255) DEFAULT NULL,
  english_name varchar(255) DEFAULT NULL,
  enterprises_type varchar(50) DEFAULT NULL,
  founded_time datetime(6) DEFAULT NULL,
  industry varchar(32) DEFAULT NULL,
  industry_paqu varchar(1000) DEFAULT NULL,
  insured_num varchar(32) DEFAULT NULL,
  product_label varchar(512) DEFAULT NULL,
  province varchar(32) DEFAULT NULL,
  registered_capital varchar(50) DEFAULT NULL,
  representative varchar(32) DEFAULT NULL,
  taxpayer_num varchar(50) DEFAULT NULL,
  telephone varchar(255) DEFAULT NULL,
  website varchar(500) DEFAULT NULL,
  industry_web_label varchar(512) DEFAULT NULL COMMENT '国标行业标签',
  industry_domain_label varchar(512) DEFAULT NULL COMMENT '素问产业链标签',
  scale varchar(255) DEFAULT NULL COMMENT '规模标签',
  policy_type varchar(512) DEFAULT NULL COMMENT '政策类型标签',
  PRIMARY KEY (id),
  UNIQUE KEY company_full_name_idx (full_name),
  UNIQUE KEY company_short_name_idx (short_name),
  KEY company_board_id_idx (board_id),
  KEY company_city_idx (city),
  KEY company_province_idx (province)
)
`,
			`CREATE TABLE company_patent (
  id int NOT NULL,
  company_id int DEFAULT NULL,
  name varchar(512) DEFAULT NULL,
  status varchar(50) DEFAULT NULL,
  patent_type varchar(50) DEFAULT NULL,
  num varchar(100) DEFAULT NULL,
  public_date date DEFAULT NULL,
  application_date date DEFAULT NULL,
  update_time datetime NOT NULL DEFAULT current_timestamp() ON UPDATE current_timestamp() COMMENT '最后修改时间',
  PRIMARY KEY (id),
  KEY company_id_idx (company_id),
  KEY status_idx (status)
)
`,
			`CREATE TABLE news_company (
  id int NOT NULL,
  company_id int DEFAULT NULL,
  news_id int DEFAULT NULL,
  release_time datetime DEFAULT NULL,
  extract_type varchar(50) DEFAULT NULL,
  is_hot smallint DEFAULT NULL,
  polarity smallint DEFAULT NULL,
  update_time datetime NOT NULL DEFAULT current_timestamp() ON UPDATE current_timestamp() COMMENT '最后修改时间',
  PRIMARY KEY (id),
  KEY company_id_idx (company_id),
  KEY news_id_idx (news_id)
)
`,
			"INSERT INTO company(id,full_name,province) VALUES(1,'a','广东'),(2,'b','广东'),(3,'c','浙江'),(4,'d','广西'),(5,'e',NULL)",
			"INSERT INTO company_patent(id,company_id,status) VALUES(11,1,'授权'),(12,1,'授权'),(13,2,'申请'),(14,3,'授权'),(15,NULL,'授权'),(16,99,'授权')",
			"INSERT INTO news_company(id,company_id,news_id) VALUES(21,1,100),(22,1,101),(23,2,102),(24,NULL,103)",
			"CREATE TABLE tree_rows(id INT PRIMARY KEY,parent_id INT,v INT)",
			"INSERT INTO tree_rows VALUES(1,NULL,10),(2,1,20),(3,1,30)",
			"CREATE TABLE bag_rows(k INT,v INT)",
			"INSERT INTO bag_rows VALUES(NULL,7),(0,NULL),(2,10),(2,10)"} {
			execSQLRequire(t, ctx, db, statement)
		}
		read := func(t *testing.T, rows *sql.Rows, columns []string) []string {
			t.Helper()
			actual, err := rows.Columns()
			require.NoError(t, err)
			require.Equal(t, columns, actual, "identity must remain private")
			var result []string
			for rows.Next() {
				values := make([]sql.NullString, len(columns))
				args := make([]any, len(columns))
				for i := range values {
					args[i] = &values[i]
				}
				require.NoError(t, rows.Scan(args...))
				fields := make([]string, len(columns))
				for i, v := range values {
					fields[i] = "NULL"
					if v.Valid {
						fields[i] = v.String
					}
				}
				result = append(result, strings.Join(fields, ":"))
			}
			return result
		}
		for _, tc := range []struct {
			name, query   string
			columns, want []string
		}{
			{"original grouped nested IN", `SELECT c.province,
       (SELECT COUNT(DISTINCT c2.id)
        FROM company c2
        WHERE c2.province = c.province
        AND c2.id IN (
            SELECT company_id FROM company_patent WHERE status = '授权'
        )
       ) as companies_with_patents,
       COUNT(*) as total_companies
FROM company c
WHERE c.province IS NOT NULL
GROUP BY c.province
LIMIT 10;
`, []string{"province", "companies_with_patents", "total_companies"}, []string{"广东:1:2", "广西:0:1", "浙江:1:1"}},
			{"original correlated derived table", `SELECT c.full_name,
       subq.patent_count,
       subq.news_count
FROM company c
INNER JOIN (
    SELECT cp.company_id,
           COUNT(cp.id) as patent_count,
           (SELECT COUNT(*)
            FROM news_company nc
            WHERE nc.company_id = cp.company_id
           ) as news_count
    FROM company_patent cp
    WHERE cp.company_id IN (
        SELECT id FROM company WHERE province = '广东'
    )
    GROUP BY cp.company_id
) as subq ON c.id = subq.company_id
LIMIT 10;
`, []string{"full_name", "patent_count", "news_count"}, []string{"a:2:2", "b:1:1"}},
			{"EXISTS before scalar", `WITH q AS(SELECT * FROM tree_rows) SELECT b.id,
    EXISTS(SELECT 1 FROM q x WHERE x.id=b.parent_id) AS present,
    (SELECT MAX(v) FROM q x WHERE x.id<=b.id) AS m FROM q b ORDER BY b.id`, []string{"id", "present", "m"}, []string{"1:0:10", "2:1:20", "3:1:30"}},
			{"EXISTS after scalar", `WITH q AS(SELECT * FROM tree_rows) SELECT b.id,
    (SELECT MAX(v) FROM q x WHERE x.id<=b.id) AS m,
    EXISTS(SELECT 1 FROM q x WHERE x.id=b.parent_id) AS present FROM q b ORDER BY b.id`, []string{"id", "m", "present"}, []string{"1:10:0", "2:20:1", "3:30:1"}},
			{"bag NULL and consecutive aggregates", `WITH q AS(SELECT * FROM bag_rows) SELECT b.*,
    EXISTS(SELECT 1 FROM q x WHERE x.k=b.k) AS present,
    (SELECT MAX(v) FROM q x WHERE x.k<=b.k) AS m,
    (SELECT COUNT(*) FROM q x WHERE x.k<=b.k) AS c,
    (SELECT COUNT(v) FROM q x WHERE x.k<=b.k) AS cv FROM q b ORDER BY b.k,b.v`,
				[]string{"k", "v", "present", "m", "c", "cv"}, []string{"NULL:7:0:NULL:0:0", "0:NULL:1:NULL:1:0", "2:10:1:10:3:2", "2:10:1:10:3:2"}},
			{"renamed reordered columns", `WITH q(value,key_col) AS(SELECT v,k FROM bag_rows)
    SELECT b.key_col,(SELECT MAX(x.value) FROM q x WHERE x.key_col<=b.key_col) AS m FROM q b`,
				[]string{"key_col", "m"}, []string{"NULL:NULL", "0:NULL", "2:10", "2:10"}},
			{"empty inner CTE", `WITH q AS(SELECT * FROM bag_rows WHERE k<0) SELECT b.k,
    (SELECT MAX(v) FROM q x WHERE x.k<=b.k) AS m,
    (SELECT COUNT(*) FROM q x WHERE x.k<=b.k) AS c FROM bag_rows b`,
				[]string{"k", "m", "c"}, []string{"NULL:NULL:0", "0:NULL:0", "2:NULL:0", "2:NULL:0"}},
		} {
			t.Run(tc.name, func(t *testing.T) {
				rows, err := db.QueryContext(ctx, tc.query)
				require.NoError(t, err, tc.query)
				defer rows.Close()
				require.ElementsMatch(t, tc.want, read(t, rows, tc.columns))
				require.NoError(t, rows.Err())
			})
		}
		t.Run("prepared reuse after empty output and scalar error", func(t *testing.T) {
			stmt, err := db.PrepareContext(ctx, `WITH q AS(SELECT * FROM bag_rows) SELECT b.k,
    (SELECT MAX(v) FROM q x WHERE x.k<=b.k) AS m,
    (SELECT COUNT(*) FROM q x WHERE x.k<=b.k) AS c FROM q b WHERE b.k>=?`)
			require.NoError(t, err)
			defer stmt.Close()
			check := func(bound int, want []string) {
				t.Helper()
				rows, err := stmt.QueryContext(ctx, bound)
				require.NoError(t, err)
				defer rows.Close()
				require.ElementsMatch(t, want, read(t, rows, []string{"k", "m", "c"}))
				require.NoError(t, rows.Err())
			}
			check(-1, []string{"0:NULL:1", "2:10:3", "2:10:3"})
			check(3, nil)
			func() {
				rows, err := db.QueryContext(ctx, `WITH q AS(SELECT k,v FROM bag_rows UNION ALL SELECT k,v FROM bag_rows)
    SELECT (SELECT MAX(v) FROM bag_rows x WHERE x.k<=b.k) AS m FROM q b`)
				require.NoError(t, err)
				defer rows.Close()
				require.ElementsMatch(t, []string{"NULL", "NULL", "NULL", "NULL", "10", "10", "10", "10"},
					read(t, rows, []string{"m"}))
				require.NoError(t, rows.Err())
			}()
			_, err = db.ExecContext(ctx, `SELECT (SELECT x.k FROM bag_rows x WHERE x.k<=b.k)
    FROM bag_rows b WHERE b.k=2`)
			var serverErr *mysql.MySQLError
			require.ErrorAs(t, err, &serverErr)
			require.EqualValues(t, 1242, serverErr.Number)
			execSQLRequire(t, ctx, db, "INSERT INTO bag_rows VALUES(1,20)")
			check(-1, []string{"0:NULL:1", "1:20:2", "2:20:4", "2:20:4"})
		})
	})
}
