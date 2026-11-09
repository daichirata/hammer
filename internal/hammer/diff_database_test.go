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

func TestDiffAlterDatabase(t *testing.T) {
	runDiffTests(t, []diffTestCase{
		{
			name: "whitespace-only difference is no-op",
			from: `


ALTER DATABASE db SET OPTIONS(enable_key_visualizer=true);
		`,
			to: `
ALTER DATABASE db SET OPTIONS(enable_key_visualizer=true);
		`,
			expected: []string{},
		},
		{
			name: "remove database options with null",
			from: `
ALTER DATABASE db SET OPTIONS(optimizer_version=3, version_retention_period='3d', enable_key_visualizer=true);
		`,
			to: ``,
			expected: []string{
				`ALTER DATABASE db SET OPTIONS (optimizer_version = null, version_retention_period = null, enable_key_visualizer = null)`,
			},
		},
		{
			name: "set database options from empty",
			from: ``,
			to: `
ALTER DATABASE db SET OPTIONS(optimizer_version=2);
			`,
			expected: []string{
				`ALTER DATABASE db SET OPTIONS (optimizer_version = 2)`,
			},
		},
		{
			name: "update database options",
			from: `
ALTER DATABASE db SET OPTIONS(optimizer_version=3, version_retention_period='3d', enable_key_visualizer=null);
		`,
			to: `
ALTER DATABASE db SET OPTIONS(optimizer_version=2, version_retention_period='2d', enable_key_visualizer=true);
			`,
			expected: []string{
				`ALTER DATABASE db SET OPTIONS (optimizer_version = 2, version_retention_period = "2d", enable_key_visualizer = true)`,
			},
		},
		{
			name: "update to specify only optimizer_version",
			from: `
ALTER DATABASE db SET OPTIONS(optimizer_version=3, version_retention_period='3d', enable_key_visualizer=true);
		`,
			to: `
ALTER DATABASE db SET OPTIONS(optimizer_version=2);
			`,
			expected: []string{
				`ALTER DATABASE db SET OPTIONS (optimizer_version = 2, version_retention_period = null, enable_key_visualizer = null)`,
			},
		},
		{
			name: "update to specify only version_retention_period",
			from: `
ALTER DATABASE db SET OPTIONS(optimizer_version=3, version_retention_period='3d', enable_key_visualizer=true);
		`,
			to: `
ALTER DATABASE db SET OPTIONS(version_retention_period='4d');
			`,
			expected: []string{
				`ALTER DATABASE db SET OPTIONS (version_retention_period = "4d", optimizer_version = null, enable_key_visualizer = null)`,
			},
		},
		{
			name: "update to specify only enable_key_visualizer",
			from: `
ALTER DATABASE db SET OPTIONS(optimizer_version=3, version_retention_period='3d');
		`,
			to: `
ALTER DATABASE db SET OPTIONS(enable_key_visualizer=true);
			`,
			expected: []string{
				`ALTER DATABASE db SET OPTIONS (enable_key_visualizer = true, optimizer_version = null, version_retention_period = null)`,
			},
		},
		{
			name: "ignore alter database diffs",
			from: `
ALTER DATABASE db SET OPTIONS(optimizer_version=3, version_retention_period='3d');
CREATE TABLE t1 (
  t1_1 INT64 NOT NULL,
) PRIMARY KEY(t1_1);
		`,
			to: `
CREATE TABLE t1 (
  t1_1 INT64 NOT NULL,
) PRIMARY KEY(t1_1);
			`,
			ignoreAlterDatabase: true,
			expected:            []string{},
		},
	})
}

func TestDiffProtoBundle(t *testing.T) {
	runDiffTests(t, []diffTestCase{
		{
			name: "ignore create proto bundle in from",
			from: `
CREATE TABLE t1 (
  t1_1 INT64 NOT NULL,
) PRIMARY KEY(t1_1);
CREATE PROTO BUNDLE (foo.bar);
`,
			to: `
CREATE TABLE t1 (
  t1_1 INT64 NOT NULL,
) PRIMARY KEY(t1_1);
`,
			ignoreProtoBundles: true,
			expected:           []string{},
		},
		{
			name: "ignore create proto bundle in to",
			from: `
CREATE TABLE t1 (
  t1_1 INT64 NOT NULL,
) PRIMARY KEY(t1_1);
`,
			to: `
CREATE TABLE t1 (
  t1_1 INT64 NOT NULL,
) PRIMARY KEY(t1_1);
CREATE PROTO BUNDLE (foo.bar);
`,
			ignoreProtoBundles: true,
			expected:           []string{},
		},
		{
			name: "ignore create proto bundle in both",
			from: `
CREATE TABLE t1 (
  t1_1 INT64 NOT NULL,
) PRIMARY KEY(t1_1);
CREATE PROTO BUNDLE (foo.bar);
`,
			to: `
CREATE TABLE t1 (
  t1_1 INT64 NOT NULL,
) PRIMARY KEY(t1_1);
CREATE PROTO BUNDLE (foo.bar, baz.qux);
`,
			ignoreProtoBundles: true,
			expected:           []string{},
		},
	})
}
