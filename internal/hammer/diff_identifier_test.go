package hammer_test

import "testing"

func TestDiffIdentifier(t *testing.T) {
	runDiffTests(t, []diffTestCase{
		{
			name: "table and column names are not case-sensitive",
			from: `
CREATE TABLE t1 (
	t1_1 INT64 NOT NULL,
	t1_2 INT64 NOT NULL,
) PRIMARY KEY(t1_1);
`,
			to: `
CREATE TABLE T1 (
	t1_1 INT64 NOT NULL,
	T1_2 INT64 NOT NULL,
) PRIMARY KEY(t1_1);
`,
			expected: []string{},
		},
		{
			name: "named schema",
			from: `
			CREATE TABLE schema.t1 (
				t1_1 INT64 NOT NULL,
			) PRIMARY KEY(t1_1);
			CREATE INDEX schema.idx_t1_1 ON schema.t1(t1_1);
			`,
			to: `
			CREATE TABLE schema.t1 (
				t1_1 INT64 NOT NULL,
				t1_2 INT64,
			) PRIMARY KEY(t1_1);
			`,
			expected: []string{
				"DROP INDEX schema.idx_t1_1",
				"ALTER TABLE schema.t1 ADD COLUMN t1_2 INT64",
			},
		},
		{
			name: "keyword identifier",
			from: `
			CREATE TABLE ` + "`Order`" + ` (
				order_1 INT64 NOT NULL,
			) PRIMARY KEY(order_1);
			`,
			to: `
			CREATE TABLE ` + "`Order`" + ` (
				order_1 INT64 NOT NULL,
				order_2 INT64,
			) PRIMARY KEY(order_1);
			`,
			expected: []string{
				"ALTER TABLE `Order` ADD COLUMN order_2 INT64",
			},
		},
	})
}
