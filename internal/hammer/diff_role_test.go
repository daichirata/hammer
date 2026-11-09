package hammer_test

import "testing"

func TestDiffRole(t *testing.T) {
	runDiffTests(t, []diffTestCase{
		{
			name: "create role",
			from: `
			CREATE ROLE role1;
			`,
			to: `
			CREATE ROLE role1;
			CREATE ROLE role2;
			`,
			expected: []string{
				`CREATE ROLE role2`,
			},
		},
		{
			name: "drop role",
			from: `
			CREATE ROLE role1;
			CREATE ROLE role2;
			`,
			to: `
			CREATE ROLE role1;
`,
			expected: []string{
				`DROP ROLE role2`,
			},
		},
		{
			name: "drop unused role only",
			from: `
			CREATE ROLE role1;
			CREATE ROLE role2;
			GRANT SELECT ON TABLE T1 TO ROLE role1;
			`,
			to: `
			CREATE ROLE role1;
			GRANT SELECT ON TABLE T1 TO ROLE role1;
			`,
			expected: []string{
				`DROP ROLE role2`,
			},
		},
	})
}

func TestDiffGrant(t *testing.T) {
	runDiffTests(t, []diffTestCase{
		{
			name: "grant role",
			from: `
			GRANT SELECT ON TABLE T1 TO ROLE role1;
			`,
			to: `
			GRANT SELECT ON TABLE T1 TO ROLE role1;
			GRANT SELECT ON TABLE T2 TO ROLE role2;
			`,
			expected: []string{
				`GRANT SELECT ON TABLE T2 TO ROLE role2`,
			},
		},
		{
			name: "grant select on view",
			from: `
				CREATE ROLE role1;
			`,
			to: `
				CREATE ROLE role1;
				GRANT SELECT ON VIEW V1 TO ROLE role1;
			`,
			expected: []string{
				`GRANT SELECT ON VIEW V1 TO ROLE role1`,
			},
		},
		{
			name: "grant select on change stream",
			from: `
				CREATE ROLE role1;
			`,
			to: `
				CREATE ROLE role1;
				GRANT SELECT ON CHANGE STREAM cs1 TO ROLE role1;
			`,
			expected: []string{
				`GRANT SELECT ON CHANGE STREAM cs1 TO ROLE role1`,
			},
		},
		{
			name: "grant execute on table function",
			from: `
				CREATE ROLE role1;
			`,
			to: `
				CREATE ROLE role1;
				GRANT EXECUTE ON TABLE FUNCTION tf1 TO ROLE role1;
			`,
			expected: []string{
				`GRANT EXECUTE ON TABLE FUNCTION tf1 TO ROLE role1`,
			},
		},
		{
			name: "grant role with same roles in different order",
			from: `
			GRANT SELECT ON TABLE T1 TO ROLE role1, role2;
			`,
			to: `
			GRANT SELECT ON TABLE T1 TO ROLE role2, role1;
			`,
			expected: []string{
				`REVOKE SELECT ON TABLE T1 FROM ROLE role1, role2`,
				`GRANT SELECT ON TABLE T1 TO ROLE role2, role1`,
			},
		},
		{
			name: "revoke role",
			from: `
			GRANT SELECT ON TABLE T1 TO ROLE role1;
			GRANT SELECT ON TABLE T2 TO ROLE role2;
			`,
			to: `
			GRANT SELECT ON TABLE T1 TO ROLE role1;
			`,
			expected: []string{
				`REVOKE SELECT ON TABLE T2 FROM ROLE role2`,
			},
		},
		{
			name: "replace grant role",
			from: `
			GRANT SELECT ON TABLE T1 TO ROLE role1;
			`,
			to: `
			GRANT SELECT, INSERT ON TABLE T1, T2 TO ROLE role1, role2;
			`,
			expected: []string{
				`REVOKE SELECT ON TABLE T1 FROM ROLE role1`,
				`GRANT SELECT, INSERT ON TABLE T1, T2 TO ROLE role1, role2`,
			},
		},
		{
			name: "revoke only, keep role",
			from: `
			CREATE ROLE role1;
			GRANT SELECT ON TABLE T1 TO ROLE role1;
			`,
			to: `
			CREATE ROLE role1;
			`,
			expected: []string{
				`REVOKE SELECT ON TABLE T1 FROM ROLE role1`,
			},
		},
		{
			name: "grant same role on different tables",
			from: `
			CREATE ROLE role1;
			GRANT SELECT ON TABLE T1 TO ROLE role1;
			`,
			to: `
			CREATE ROLE role1;
			GRANT SELECT ON TABLE T1 TO ROLE role1;
			GRANT SELECT ON TABLE T2 TO ROLE role1;
			`,
			expected: []string{
				`GRANT SELECT ON TABLE T2 TO ROLE role1`,
			},
		},
		{
			name: "revoke same role on different tables",
			from: `
			CREATE ROLE role1;
			GRANT SELECT ON TABLE T1 TO ROLE role1;
			GRANT SELECT ON TABLE T2 TO ROLE role1;
			`,
			to: `
			CREATE ROLE role1;
			GRANT SELECT ON TABLE T1 TO ROLE role1;
			`,
			expected: []string{
				`REVOKE SELECT ON TABLE T2 FROM ROLE role1`,
			},
		},
		{
			name: "grant on multiple tables in different order",
			from: `
				GRANT SELECT ON TABLE T1, T2 TO ROLE role1;
			`,
			to: `
				GRANT SELECT ON TABLE T2, T1 TO ROLE role1;
			`,
			expected: []string{
				`REVOKE SELECT ON TABLE T1, T2 FROM ROLE role1`,
				`GRANT SELECT ON TABLE T2, T1 TO ROLE role1`,
			},
		},
		{
			name: "replace privilege type on same table",
			from: `
				GRANT SELECT ON TABLE T1 TO ROLE role1;
			`,
			to: `
				GRANT INSERT ON TABLE T1 TO ROLE role1;
			`,
			expected: []string{
				`REVOKE SELECT ON TABLE T1 FROM ROLE role1`,
				`GRANT INSERT ON TABLE T1 TO ROLE role1`,
			},
		},
		{
			name: "grant multiple privileges at once",
			from: ``,
			to: `
				GRANT SELECT, INSERT, DELETE ON TABLE T1 TO ROLE role1;
			`,
			expected: []string{
				`GRANT SELECT, INSERT, DELETE ON TABLE T1 TO ROLE role1`,
			},
		},
		{
			name: "revoke select column and grant new columns",
			from: `
				GRANT SELECT(col1) ON TABLE T1 TO ROLE role1;
			`,
			to: `
				GRANT SELECT(col1, col2) ON TABLE T1 TO ROLE role1;
			`,
			expected: []string{
				`REVOKE SELECT(col1) ON TABLE T1 FROM ROLE role1`,
				`GRANT SELECT(col1, col2) ON TABLE T1 TO ROLE role1`,
			},
		},
		{
			name: "grant insert with column list",
			from: `
CREATE TABLE t1 (
  id INT64 NOT NULL,
  a INT64,
  b INT64,
) PRIMARY KEY(id);
CREATE ROLE r1;
`,
			to: `
CREATE TABLE t1 (
  id INT64 NOT NULL,
  a INT64,
  b INT64,
) PRIMARY KEY(id);
CREATE ROLE r1;
GRANT INSERT(a, b) ON TABLE t1 TO ROLE r1;
`,
			expected: []string{
				"GRANT INSERT(a, b) ON TABLE t1 TO ROLE r1",
			},
		},
		{
			name: "grant update with column list",
			from: `
CREATE TABLE t1 (
  id INT64 NOT NULL,
  a INT64,
  b INT64,
) PRIMARY KEY(id);
CREATE ROLE r1;
`,
			to: `
CREATE TABLE t1 (
  id INT64 NOT NULL,
  a INT64,
  b INT64,
) PRIMARY KEY(id);
CREATE ROLE r1;
GRANT UPDATE(a) ON TABLE t1 TO ROLE r1;
`,
			expected: []string{
				"GRANT UPDATE(a) ON TABLE t1 TO ROLE r1",
			},
		},
		{
			name: "grant delete privilege",
			from: `
CREATE TABLE t1 (
  id INT64 NOT NULL,
) PRIMARY KEY(id);
CREATE ROLE r1;
`,
			to: `
CREATE TABLE t1 (
  id INT64 NOT NULL,
) PRIMARY KEY(id);
CREATE ROLE r1;
GRANT DELETE ON TABLE t1 TO ROLE r1;
`,
			expected: []string{
				"GRANT DELETE ON TABLE t1 TO ROLE r1",
			},
		},
		{
			name: "revoke on table before DROP TABLE",
			from: `
				CREATE ROLE role1;
				CREATE TABLE T1 (id INT64);
				GRANT SELECT ON TABLE T1 TO ROLE role1;
			`,
			to: `
				CREATE ROLE role1;
			`,
			expected: []string{
				`DROP TABLE T1`,
			},
		},
		{
			name: "revoke on table before DROP TABLE (PK change triggers drop)",
			from: `
				CREATE ROLE role1;
				CREATE TABLE T1 (
  id INT64,
  name STRING(100),
) PRIMARY KEY(id);
				GRANT SELECT ON TABLE T1 TO ROLE role1;
			`,
			to: `
				CREATE ROLE role1;
				CREATE TABLE T1 (
  id INT64,
  name STRING(100),
) PRIMARY KEY(name);
				GRANT SELECT ON TABLE T1 TO ROLE role1;
			`,
			expected: []string{
				`DROP TABLE T1`,
				`
CREATE TABLE T1 (
  id INT64,
  name STRING(100)
) PRIMARY KEY (name)`,
				`GRANT SELECT ON TABLE T1 TO ROLE role1`,
			},
		},
		{
			name: "revoke on table before DROP TABLE (interleave parent changed)",
			from: `
				CREATE ROLE role1;
				CREATE TABLE P1 (
  pid INT64 NOT NULL,
) PRIMARY KEY(pid);
				CREATE TABLE P2 (
  pid INT64 NOT NULL,
) PRIMARY KEY(pid);
				CREATE TABLE T1 (
  id INT64 NOT NULL,
) PRIMARY KEY(id), INTERLEAVE IN PARENT P1;
				GRANT SELECT ON TABLE T1 TO ROLE role1;
			`,
			to: `
				CREATE ROLE role1;
				CREATE TABLE P1 (
  pid INT64 NOT NULL,
) PRIMARY KEY(pid);
				CREATE TABLE P2 (
  pid INT64 NOT NULL,
) PRIMARY KEY(pid);
				CREATE TABLE T1 (
  id INT64 NOT NULL,
) PRIMARY KEY(id), INTERLEAVE IN PARENT P2;
				GRANT SELECT ON TABLE T1 TO ROLE role1;
			`,
			expected: []string{
				`DROP TABLE T1`,
				`
CREATE TABLE T1 (
  id INT64 NOT NULL
) PRIMARY KEY (id),
  INTERLEAVE IN PARENT P2`,
				`GRANT SELECT ON TABLE T1 TO ROLE role1`,
			},
		},
		{
			name: "revoke on table before DROP TABLE (removing interleave)",
			from: `
				CREATE ROLE role1;
				CREATE TABLE P1 (
  pid INT64 NOT NULL,
) PRIMARY KEY(pid);
				CREATE TABLE T1 (
  id INT64 NOT NULL,
) PRIMARY KEY(id), INTERLEAVE IN PARENT P1;
				GRANT SELECT ON TABLE T1 TO ROLE role1;
			`,
			to: `
				CREATE ROLE role1;
				CREATE TABLE P1 (
  pid INT64 NOT NULL,
) PRIMARY KEY(pid);
				CREATE TABLE T1 (
  id INT64 NOT NULL,
) PRIMARY KEY(id);
				GRANT SELECT ON TABLE T1 TO ROLE role1;
			`,
			expected: []string{
				`DROP TABLE T1`,
				`
CREATE TABLE T1 (
  id INT64 NOT NULL
) PRIMARY KEY (id)`,
				`GRANT SELECT ON TABLE T1 TO ROLE role1`,
			},
		},
		{
			name: "revoke on column before DROP COLUMN",
			from: `
				CREATE ROLE role1;
				CREATE TABLE T1 (id INT64, name STRING(100));
				GRANT SELECT(name) ON TABLE T1 TO ROLE role1;
			`,
			to: `
				CREATE ROLE role1;
				CREATE TABLE T1 (id INT64);
			`,
			expected: []string{
				`ALTER TABLE T1 DROP COLUMN name`,
			},
		},
		{
			name: "revoke on view before DROP VIEW",
			from: `
				CREATE ROLE role1;
				CREATE VIEW V1 SQL SECURITY INVOKER AS SELECT 1;
				GRANT SELECT ON VIEW V1 TO ROLE role1;
			`,
			to: `
				CREATE ROLE role1;
			`,
			expected: []string{
				`DROP VIEW V1`,
			},
		},
		{
			name: "revoke on change stream before DROP CHANGE STREAM",
			from: `
				CREATE ROLE role1;
				CREATE CHANGE STREAM CS1 FOR ALL;
				GRANT SELECT ON CHANGE STREAM CS1 TO ROLE role1;
			`,
			to: `
				CREATE ROLE role1;
			`,
			expected: []string{
				`DROP CHANGE STREAM CS1`,
			},
		},
		{
			name: "revoke table grant before DROP ROLE",
			from: `
				CREATE ROLE role1;
				CREATE TABLE T1 (id INT64);
				GRANT SELECT ON TABLE T1 TO ROLE role1;
			`,
			to: `
				CREATE TABLE T1 (id INT64);
			`,
			expected: []string{
				`REVOKE SELECT ON TABLE T1 FROM ROLE role1`,
				`DROP ROLE role1`,
			},
		},
		{
			name: "revoke view grant before DROP ROLE",
			from: `
				CREATE ROLE role1;
				CREATE VIEW V1 SQL SECURITY INVOKER AS SELECT 1;
				GRANT SELECT ON VIEW V1 TO ROLE role1;
			`,
			to: `
				CREATE VIEW V1 SQL SECURITY INVOKER AS SELECT 1;
			`,
			expected: []string{
				`REVOKE SELECT ON VIEW V1 FROM ROLE role1`,
				`DROP ROLE role1`,
			},
		},
		{
			name: "revoke change stream grant before DROP ROLE",
			from: `
				CREATE ROLE role1;
				CREATE CHANGE STREAM CS1 FOR ALL;
				GRANT SELECT ON CHANGE STREAM CS1 TO ROLE role1;
			`,
			to: `
				CREATE CHANGE STREAM CS1 FOR ALL;
			`,
			expected: []string{
				`REVOKE SELECT ON CHANGE STREAM CS1 FROM ROLE role1`,
				`DROP ROLE role1`,
			},
		},
		{
			name: "drop role: revoke only for resources that remain in target",
			from: `
				CREATE ROLE role1;
				CREATE TABLE T1 (id INT64);
				CREATE TABLE T2 (id INT64);
				GRANT SELECT ON TABLE T1 TO ROLE role1;
				GRANT SELECT ON TABLE T2 TO ROLE role1;
			`,
			to: `
				CREATE TABLE T1 (id INT64);
			`,
			expected: []string{
				`DROP TABLE T2`,
				`REVOKE SELECT ON TABLE T1 FROM ROLE role1`,
				`DROP ROLE role1`,
			},
		},
		{
			name: "drop a column of a grant on multiple columns",
			from: `
CREATE ROLE R;
CREATE TABLE T (Id INT64 NOT NULL, C1 INT64, C2 INT64) PRIMARY KEY (Id);
GRANT SELECT(C1, C2) ON TABLE T TO ROLE R;
`,
			to: `
CREATE ROLE R;
CREATE TABLE T (Id INT64 NOT NULL, C2 INT64) PRIMARY KEY (Id);
GRANT SELECT(C2) ON TABLE T TO ROLE R;
`,
			expected: []string{
				`REVOKE SELECT(C1, C2) ON TABLE T FROM ROLE R`,
				`ALTER TABLE T DROP COLUMN C1`,
				`GRANT SELECT(C2) ON TABLE T TO ROLE R`,
			},
		},
		{
			name: "drop a column of a grant with a table-level privilege",
			from: `
CREATE ROLE R;
CREATE TABLE T (Id INT64 NOT NULL, C1 INT64, C2 INT64) PRIMARY KEY (Id);
GRANT SELECT, UPDATE(C1) ON TABLE T TO ROLE R;
`,
			to: `
CREATE ROLE R;
CREATE TABLE T (Id INT64 NOT NULL, C2 INT64) PRIMARY KEY (Id);
GRANT SELECT ON TABLE T TO ROLE R;
`,
			expected: []string{
				`REVOKE SELECT, UPDATE(C1) ON TABLE T FROM ROLE R`,
				`ALTER TABLE T DROP COLUMN C1`,
				`GRANT SELECT ON TABLE T TO ROLE R`,
			},
		},
		{
			name: "drop the only column of a grant",
			from: `
CREATE ROLE R;
CREATE TABLE T (Id INT64 NOT NULL, C1 INT64, C2 INT64) PRIMARY KEY (Id);
GRANT SELECT(C1) ON TABLE T TO ROLE R;
`,
			to: `
CREATE ROLE R;
CREATE TABLE T (Id INT64 NOT NULL, C2 INT64) PRIMARY KEY (Id);
`,
			expected: []string{
				`ALTER TABLE T DROP COLUMN C1`,
			},
		},
		{
			name: "drop a column and a role of a grant on multiple columns",
			from: `
CREATE ROLE R;
CREATE TABLE T (Id INT64 NOT NULL, C1 INT64, C2 INT64) PRIMARY KEY (Id);
GRANT SELECT(C1, C2) ON TABLE T TO ROLE R;
`,
			to: `
CREATE TABLE T (Id INT64 NOT NULL, C2 INT64) PRIMARY KEY (Id);
`,
			expected: []string{
				`REVOKE SELECT(C1, C2) ON TABLE T FROM ROLE R`,
				`ALTER TABLE T DROP COLUMN C1`,
				`DROP ROLE R`,
			},
		},
		{
			name: "change the type of a column of a grant on multiple columns",
			from: `
CREATE ROLE R;
CREATE TABLE T (Id INT64 NOT NULL, C1 INT64, C2 INT64) PRIMARY KEY (Id);
GRANT SELECT(C1, C2) ON TABLE T TO ROLE R;
`,
			to: `
CREATE ROLE R;
CREATE TABLE T (Id INT64 NOT NULL, C1 STRING(10), C2 INT64) PRIMARY KEY (Id);
GRANT SELECT(C1, C2) ON TABLE T TO ROLE R;
`,
			expected: []string{
				`REVOKE SELECT(C1, C2) ON TABLE T FROM ROLE R`,
				`ALTER TABLE T DROP COLUMN C1`,
				`ALTER TABLE T ADD COLUMN C1 STRING(10)`,
				`GRANT SELECT(C1, C2) ON TABLE T TO ROLE R`,
			},
		},
	})
}
