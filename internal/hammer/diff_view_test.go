package hammer_test

import "testing"

func TestDiffView(t *testing.T) {
	runDiffTests(t, []diffTestCase{
		{
			name: "drop view",
			from: `
CREATE TABLE t1 (
	t1_1 INT64 NOT NULL,
) PRIMARY KEY(t1_1);

CREATE VIEW v1
SQL SECURITY INVOKER
AS SELECT * FROM t1;
`,
			to: `
CREATE TABLE t1 (
	t1_1 INT64 NOT NULL,
) PRIMARY KEY(t1_1);
`,
			expected: []string{
				`DROP VIEW v1`,
			},
		},
		{
			name: "create view",
			from: `
CREATE TABLE t1 (
	t1_1 INT64 NOT NULL,
) PRIMARY KEY(t1_1);
`,
			to: `
CREATE TABLE t1 (
	t1_1 INT64 NOT NULL,
) PRIMARY KEY(t1_1);

CREATE VIEW v1
SQL SECURITY INVOKER
AS SELECT * FROM t1;
`,
			expected: []string{
				`CREATE VIEW v1 SQL SECURITY INVOKER AS SELECT * FROM t1`,
			},
		},
		{
			name: "replace view",
			from: `
CREATE TABLE t1 (
	t1_1 INT64 NOT NULL,
) PRIMARY KEY(t1_1);

CREATE VIEW v1
SQL SECURITY INVOKER
AS SELECT * FROM t1 WHERE t1_1 > 0;
`,
			to: `
CREATE TABLE t1 (
	t1_1 INT64 NOT NULL,
) PRIMARY KEY(t1_1);

CREATE VIEW v1
SQL SECURITY INVOKER
AS SELECT * FROM t1;
`,
			expected: []string{
				`CREATE OR REPLACE VIEW v1 SQL SECURITY INVOKER AS SELECT * FROM t1`,
			},
		},
		{
			name: "view without changes",
			from: `
CREATE TABLE t1 (t1_1 INT64 NOT NULL) PRIMARY KEY (t1_1);
CREATE VIEW v1 SQL SECURITY INVOKER AS SELECT * FROM t1;
`,
			to: `
CREATE TABLE t1 (t1_1 INT64 NOT NULL) PRIMARY KEY (t1_1);
CREATE VIEW v1
SQL SECURITY INVOKER
AS select *
from t1;
`,
			expected: []string{},
		},
		{
			name: "view with grant without changes",
			from: `
CREATE ROLE r1;
CREATE TABLE t1 (t1_1 INT64 NOT NULL) PRIMARY KEY (t1_1);
CREATE VIEW v1 SQL SECURITY INVOKER AS SELECT * FROM t1;
GRANT SELECT ON VIEW v1 TO ROLE r1;
`,
			to: `
CREATE ROLE r1;
CREATE TABLE t1 (t1_1 INT64 NOT NULL) PRIMARY KEY (t1_1);
CREATE VIEW v1 SQL SECURITY INVOKER AS SELECT * FROM t1;
GRANT SELECT ON VIEW v1 TO ROLE r1;
`,
			expected: []string{},
		},
		{
			name: "replace view regrants the view",
			from: `
CREATE ROLE r1;
CREATE TABLE t1 (t1_1 INT64 NOT NULL) PRIMARY KEY (t1_1);
CREATE VIEW v1 SQL SECURITY INVOKER AS SELECT * FROM t1 WHERE t1_1 > 0;
GRANT SELECT ON VIEW v1 TO ROLE r1;
`,
			to: `
CREATE ROLE r1;
CREATE TABLE t1 (t1_1 INT64 NOT NULL) PRIMARY KEY (t1_1);
CREATE VIEW v1 SQL SECURITY INVOKER AS SELECT * FROM t1;
GRANT SELECT ON VIEW v1 TO ROLE r1;
`,
			expected: []string{
				`CREATE OR REPLACE VIEW v1 SQL SECURITY INVOKER AS SELECT * FROM t1`,
				`GRANT SELECT ON VIEW v1 TO ROLE r1`,
			},
		},
		{
			name: "replace view and remove its grant",
			from: `
CREATE ROLE r1;
CREATE TABLE t1 (t1_1 INT64 NOT NULL) PRIMARY KEY (t1_1);
CREATE VIEW v1 SQL SECURITY INVOKER AS SELECT * FROM t1 WHERE t1_1 > 0;
GRANT SELECT ON VIEW v1 TO ROLE r1;
`,
			to: `
CREATE ROLE r1;
CREATE TABLE t1 (t1_1 INT64 NOT NULL) PRIMARY KEY (t1_1);
CREATE VIEW v1 SQL SECURITY INVOKER AS SELECT * FROM t1;
`,
			expected: []string{
				`CREATE OR REPLACE VIEW v1 SQL SECURITY INVOKER AS SELECT * FROM t1`,
			},
		},
		{
			name: "replace view and drop role with grant on the view",
			from: `
CREATE ROLE r1;
CREATE TABLE t1 (t1_1 INT64 NOT NULL) PRIMARY KEY (t1_1);
CREATE VIEW v1 SQL SECURITY INVOKER AS SELECT * FROM t1 WHERE t1_1 > 0;
GRANT SELECT ON VIEW v1 TO ROLE r1;
`,
			to: `
CREATE TABLE t1 (t1_1 INT64 NOT NULL) PRIMARY KEY (t1_1);
CREATE VIEW v1 SQL SECURITY INVOKER AS SELECT * FROM t1;
`,
			expected: []string{
				`CREATE OR REPLACE VIEW v1 SQL SECURITY INVOKER AS SELECT * FROM t1`,
				`DROP ROLE r1`,
			},
		},
		{
			name: "replace definer view",
			from: `
CREATE TABLE t1 (t1_1 INT64 NOT NULL) PRIMARY KEY (t1_1);
CREATE VIEW v1 SQL SECURITY DEFINER AS SELECT t1.t1_1 FROM t1 WHERE t1.t1_1 > 0;
`,
			to: `
CREATE TABLE t1 (t1_1 INT64 NOT NULL) PRIMARY KEY (t1_1);
CREATE VIEW v1 SQL SECURITY DEFINER AS SELECT t1.t1_1 FROM t1;
`,
			expected: []string{
				`CREATE OR REPLACE VIEW v1 SQL SECURITY DEFINER AS SELECT t1.t1_1 FROM t1`,
			},
		},
		{
			name: "change view security type",
			from: `
CREATE TABLE t1 (t1_1 INT64 NOT NULL) PRIMARY KEY (t1_1);
CREATE VIEW v1 SQL SECURITY INVOKER AS SELECT t1.t1_1 FROM t1;
`,
			to: `
CREATE TABLE t1 (t1_1 INT64 NOT NULL) PRIMARY KEY (t1_1);
CREATE VIEW v1 SQL SECURITY DEFINER AS SELECT t1.t1_1 FROM t1;
`,
			expected: []string{
				`CREATE OR REPLACE VIEW v1 SQL SECURITY DEFINER AS SELECT t1.t1_1 FROM t1`,
			},
		},
		{
			name: "definer view without changes",
			from: `
CREATE TABLE t1 (t1_1 INT64 NOT NULL) PRIMARY KEY (t1_1);
CREATE VIEW v1 SQL SECURITY DEFINER AS SELECT t1.t1_1 FROM t1;
`,
			to: `
CREATE TABLE t1 (t1_1 INT64 NOT NULL) PRIMARY KEY (t1_1);
CREATE VIEW v1 SQL SECURITY DEFINER AS SELECT t1.t1_1 FROM t1;
`,
			expected: []string{},
		},
		{
			name: "drop view before table it references",
			from: `
CREATE TABLE t1 (
	t1_1 INT64 NOT NULL,
) PRIMARY KEY(t1_1);

CREATE VIEW v1
SQL SECURITY INVOKER
AS SELECT * FROM t1;
`,
			to: ``,
			expected: []string{
				`DROP VIEW v1`,
				`DROP TABLE t1`,
			},
		},
		{
			name: "drop view before column it references",
			from: `
CREATE TABLE t1 (
	t1_1 INT64 NOT NULL,
	t1_2 INT64 NOT NULL,
) PRIMARY KEY(t1_1);

CREATE VIEW v1
SQL SECURITY INVOKER
AS SELECT t1_2 FROM t1;
`,
			to: `
CREATE TABLE t1 (
	t1_1 INT64 NOT NULL,
) PRIMARY KEY(t1_1);
`,
			expected: []string{
				`DROP VIEW v1`,
				`ALTER TABLE t1 DROP COLUMN t1_2`,
			},
		},
		{
			name: "drop views in reverse definition order",
			from: `
CREATE TABLE t1 (
	t1_1 INT64 NOT NULL,
) PRIMARY KEY(t1_1);

CREATE VIEW v1
SQL SECURITY INVOKER
AS SELECT t1_1 FROM t1;

CREATE VIEW v2
SQL SECURITY INVOKER
AS SELECT t1_1 FROM v1;
`,
			to: ``,
			expected: []string{
				`DROP VIEW v2`,
				`DROP VIEW v1`,
				`DROP TABLE t1`,
			},
		},
	})
}
