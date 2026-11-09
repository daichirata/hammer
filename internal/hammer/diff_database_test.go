package hammer_test

import "testing"

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
