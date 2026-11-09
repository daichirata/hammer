package hammer_test

import "testing"

func TestDiffSchema(t *testing.T) {
	runDiffTests(t, []diffTestCase{
		{
			name: "create schema",
			from: ``,
			to: `
			CREATE SCHEMA sch1;
			`,
			expected: []string{
				`CREATE SCHEMA sch1`,
			},
		},
		{
			name: "drop schema",
			from: `
			CREATE SCHEMA sch1;
			CREATE SCHEMA sch2;
			`,
			to: `
			CREATE SCHEMA sch1;
			`,
			expected: []string{
				`DROP SCHEMA sch2`,
			},
		},
		{
			name: "schema unchanged is a no-op",
			from: `
			CREATE SCHEMA sch1;
			`,
			to: `
			CREATE SCHEMA sch1;
			`,
			expected: nil,
		},
		{
			name: "create schema before table in that schema",
			from: ``,
			to: `
			CREATE SCHEMA sch1;
			CREATE TABLE sch1.t1 (
			  t1_1 INT64 NOT NULL,
			) PRIMARY KEY(t1_1);
			`,
			expected: []string{
				`CREATE SCHEMA sch1`,
				`
CREATE TABLE sch1.t1 (
  t1_1 INT64 NOT NULL
) PRIMARY KEY (t1_1)`,
			},
		},
		{
			name: "drop schema after table in that schema",
			from: `
			CREATE SCHEMA sch1;
			CREATE TABLE sch1.t1 (
			  t1_1 INT64 NOT NULL,
			) PRIMARY KEY(t1_1);
			`,
			to: ``,
			expected: []string{
				`DROP TABLE sch1.t1`,
				`DROP SCHEMA sch1`,
			},
		},
		{
			name: "create schema before schema-qualified table, index and view",
			from: ``,
			to: `
			CREATE SCHEMA sch1;
			CREATE TABLE sch1.t1 (
			  id INT64 NOT NULL,
			  name STRING(MAX),
			) PRIMARY KEY(id);
			CREATE INDEX sch1.idx1 ON sch1.t1(name);
			CREATE VIEW sch1.v1 SQL SECURITY INVOKER AS SELECT id FROM sch1.t1;
			`,
			expected: []string{
				`CREATE SCHEMA sch1`,
				`
CREATE TABLE sch1.t1 (
  id INT64 NOT NULL,
  name STRING(MAX)
) PRIMARY KEY (id)`,
				`CREATE INDEX sch1.idx1 ON sch1.t1(name)`,
				`CREATE VIEW sch1.v1 SQL SECURITY INVOKER AS SELECT id FROM sch1.t1`,
			},
		},
		{
			name: "drop schema after its qualified table, index and view",
			from: `
			CREATE SCHEMA sch1;
			CREATE TABLE sch1.t1 (
			  id INT64 NOT NULL,
			  name STRING(MAX),
			) PRIMARY KEY(id);
			CREATE INDEX sch1.idx1 ON sch1.t1(name);
			CREATE VIEW sch1.v1 SQL SECURITY INVOKER AS SELECT id FROM sch1.t1;
			`,
			to: ``,
			expected: []string{
				`DROP VIEW sch1.v1`,
				`DROP INDEX sch1.idx1`,
				`DROP TABLE sch1.t1`,
				`DROP SCHEMA sch1`,
			},
		},
	})
}
