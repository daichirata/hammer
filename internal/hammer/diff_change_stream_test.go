package hammer_test

import "testing"

func TestDiffChangeStream(t *testing.T) {
	runDiffTests(t, []diffTestCase{
		{
			name: "drop change stream",
			from: `
CREATE CHANGE STREAM SomeStream;
`,
			to:                  ``,
			ignoreAlterDatabase: true,
			expected:            []string{"DROP CHANGE STREAM SomeStream"},
		},
		{
			name: "drop change stream with watch all",
			from: `
CREATE CHANGE STREAM SomeStream FOR ALL;
`,
			to:                  ``,
			ignoreAlterDatabase: true,
			expected:            []string{"DROP CHANGE STREAM SomeStream"},
		},
		{
			name: "drop change stream with watch tables",
			from: `
CREATE TABLE t1 (
  t1_1 INT64 NOT NULL,
) PRIMARY KEY(t1_1);
CREATE CHANGE STREAM SomeStream FOR t1;
`,
			to: `
CREATE TABLE t1 (
  t1_1 INT64 NOT NULL,
) PRIMARY KEY(t1_1);
`,
			ignoreAlterDatabase: true,
			expected:            []string{"DROP CHANGE STREAM SomeStream"},
		},
		{
			name: "drop change stream with watch columns",
			from: `
CREATE TABLE t1 (
  t1_1 INT64 NOT NULL,
) PRIMARY KEY(t1_1);
CREATE CHANGE STREAM SomeStream FOR t1(t1_1);
`,
			to: `
CREATE TABLE t1 (
  t1_1 INT64 NOT NULL,
) PRIMARY KEY(t1_1);
`,
			ignoreAlterDatabase: true,
			expected:            []string{"DROP CHANGE STREAM SomeStream"},
		},
		{
			name: "create change stream",
			from: ``,
			to: `
CREATE CHANGE STREAM SomeStream;
`,
			ignoreAlterDatabase: true,
			expected:            []string{"CREATE CHANGE STREAM SomeStream"},
		},
		{
			name: "alter change stream watch none to all",
			from: `
CREATE CHANGE STREAM SomeStream;
`,
			to: `
CREATE CHANGE STREAM SomeStream FOR ALL;
`,
			ignoreAlterDatabase: true,
			expected:            []string{"ALTER CHANGE STREAM SomeStream SET FOR ALL"},
		},
		{
			name: "alter change stream watch none to table",
			from: `
CREATE CHANGE STREAM SomeStream;
`,
			to: `
CREATE TABLE Singers (
  id INT64 NOT NULL,
) PRIMARY KEY(id);
CREATE TABLE Albums (
  id INT64 NOT NULL,
) PRIMARY KEY(id);
CREATE CHANGE STREAM SomeStream FOR Singers(id), Albums;
`,
			ignoreAlterDatabase: true,
			expected: []string{
				`
CREATE TABLE Singers (
  id INT64 NOT NULL
) PRIMARY KEY (id)`,
				`
CREATE TABLE Albums (
  id INT64 NOT NULL
) PRIMARY KEY (id)`,
				"ALTER CHANGE STREAM SomeStream SET FOR Singers(id), Albums",
			},
		},
		{
			name: "alter change stream watch all to none",
			from: `
CREATE CHANGE STREAM SomeStream FOR ALL;
`,
			to: `
CREATE CHANGE STREAM SomeStream;
`,
			ignoreAlterDatabase: true,
			expected:            []string{"ALTER CHANGE STREAM SomeStream DROP FOR ALL"},
		},
		{
			name: "alter change stream watch all to table",
			from: `
CREATE TABLE Singers (
  id INT64 NOT NULL,
) PRIMARY KEY(id);
CREATE TABLE Albums (
  id INT64 NOT NULL,
) PRIMARY KEY(id);
CREATE CHANGE STREAM SomeStream FOR ALL;
`,
			to: `
CREATE TABLE Singers (
  id INT64 NOT NULL,
) PRIMARY KEY(id);
CREATE TABLE Albums (
  id INT64 NOT NULL,
) PRIMARY KEY(id);
CREATE CHANGE STREAM SomeStream FOR Singers(id), Albums;
`,
			ignoreAlterDatabase: true,
			expected:            []string{"ALTER CHANGE STREAM SomeStream SET FOR Singers(id), Albums"},
		},
		{
			name: "alter change stream watch table to none",
			from: `
CREATE TABLE Singers (
  id INT64 NOT NULL,
) PRIMARY KEY(id);
CREATE TABLE Albums (
  id INT64 NOT NULL,
) PRIMARY KEY(id);
CREATE CHANGE STREAM SomeStream FOR Singers(id), Albums;
`,
			to: `
CREATE TABLE Singers (
  id INT64 NOT NULL,
) PRIMARY KEY(id);
CREATE TABLE Albums (
  id INT64 NOT NULL,
) PRIMARY KEY(id);
CREATE CHANGE STREAM SomeStream;
`,
			ignoreAlterDatabase: true,
			expected:            []string{"ALTER CHANGE STREAM SomeStream DROP FOR ALL"},
		},
		{
			name: "alter change stream watch table to all",
			from: `
CREATE TABLE Singers (
  id INT64 NOT NULL,
) PRIMARY KEY(id);
CREATE TABLE Albums (
  id INT64 NOT NULL,
) PRIMARY KEY(id);
CREATE CHANGE STREAM SomeStream FOR Singers(id), Albums;
`,
			to: `
CREATE TABLE Singers (
  id INT64 NOT NULL,
) PRIMARY KEY(id);
CREATE TABLE Albums (
  id INT64 NOT NULL,
) PRIMARY KEY(id);
CREATE CHANGE STREAM SomeStream FOR All;
`,
			ignoreAlterDatabase: true,
			expected:            []string{"ALTER CHANGE STREAM SomeStream SET FOR ALL"},
		},
		{
			name: "alter change stream watch table to other table",
			from: `
CREATE TABLE Singers (
  id INT64 NOT NULL,
) PRIMARY KEY(id);
CREATE CHANGE STREAM SomeStream FOR Singers(id);
`,
			to: `
CREATE TABLE Albums (
  id INT64 NOT NULL,
) PRIMARY KEY(id);
CREATE CHANGE STREAM SomeStream FOR Albums;
`,
			ignoreAlterDatabase: true,
			expected: []string{
				`
CREATE TABLE Albums (
  id INT64 NOT NULL
) PRIMARY KEY (id)`,
				"ALTER CHANGE STREAM SomeStream SET FOR Albums",
				"DROP TABLE Singers",
			},
		},
		{
			name: "alter change stream watch column to same table",
			from: `
CREATE TABLE Singers (
  id INT64 NOT NULL,
  name STRING(MAX) NOT NULL,
) PRIMARY KEY(id);
CREATE CHANGE STREAM SomeStream FOR Singers(id);
`,
			to: `
CREATE TABLE Singers (
  id INT64 NOT NULL,
  name STRING(MAX) NOT NULL,
) PRIMARY KEY(id);
CREATE CHANGE STREAM SomeStream FOR Singers(name);
`,
			ignoreAlterDatabase: true,
			expected:            []string{"ALTER CHANGE STREAM SomeStream SET FOR Singers(name)"},
		},
		{
			name: "alter change stream watch table to same table",
			from: `
CREATE TABLE Singers (
  id INT64 NOT NULL,
) PRIMARY KEY(id);
CREATE TABLE Albums (
  id INT64 NOT NULL,
) PRIMARY KEY(id);
CREATE CHANGE STREAM SomeStream FOR Singers(id), Albums;
`,
			to: `
CREATE TABLE Singers (
  id INT64 NOT NULL,
) PRIMARY KEY(id);
CREATE TABLE Albums (
  id INT64 NOT NULL,
) PRIMARY KEY(id);
CREATE CHANGE STREAM SomeStream FOR Singers(id), Albums;
`,
			ignoreAlterDatabase: true,
			expected:            []string{},
		},
		{
			name: "delete tables and related change stream",
			from: `
CREATE TABLE Singers (
  id INT64 NOT NULL,
) PRIMARY KEY(id);
CREATE TABLE Albums (
  id INT64 NOT NULL,
) PRIMARY KEY(id);
CREATE CHANGE STREAM SomeStream FOR Singers, Albums;
`,
			to:                  ``,
			ignoreAlterDatabase: true,
			expected:            []string{"DROP CHANGE STREAM SomeStream", "DROP TABLE Singers", "DROP TABLE Albums"},
		},
		{
			name: "delete table and related change streams",
			from: `
CREATE TABLE Singers (
  id INT64 NOT NULL,
) PRIMARY KEY(id);
CREATE CHANGE STREAM SomeStreamOne FOR Singers;
CREATE CHANGE STREAM SomeStreamTwo FOR Singers;
`,
			to:                  ``,
			ignoreAlterDatabase: true,
			expected:            []string{"DROP CHANGE STREAM SomeStreamOne", "DROP CHANGE STREAM SomeStreamTwo", "DROP TABLE Singers"},
		},
		{
			name: "alter change stream option",
			from: `
CREATE CHANGE STREAM SomeStream FOR ALL OPTIONS( retention_period = '36h', value_capture_type = 'NEW_VALUES', exclude_ttl_deletes = false, exclude_insert = false, exclude_update = false, exclude_delete = false, allow_txn_exclusion = false );
`,
			to: `
CREATE CHANGE STREAM SomeStream FOR ALL OPTIONS( retention_period = '5d', value_capture_type = 'NEW_ROW', exclude_ttl_deletes = true, exclude_insert = true, exclude_update = true, exclude_delete = true, allow_txn_exclusion = true );
`,
			ignoreAlterDatabase: true,
			expected:            []string{`ALTER CHANGE STREAM SomeStream SET OPTIONS (retention_period = "5d", value_capture_type = "NEW_ROW", exclude_ttl_deletes = true, exclude_insert = true, exclude_update = true, exclude_delete = true, allow_txn_exclusion = true)`},
		},
		{
			name: "alter change stream option to default",
			from: `
CREATE CHANGE STREAM SomeStream FOR ALL OPTIONS( retention_period = '36h', value_capture_type = 'NEW_VALUES', exclude_ttl_deletes = true, exclude_insert = true, exclude_update = true, exclude_delete = true, allow_txn_exclusion = true );
`,
			to: `
CREATE CHANGE STREAM SomeStream FOR ALL;
`,
			ignoreAlterDatabase: true,
			expected:            []string{`ALTER CHANGE STREAM SomeStream SET OPTIONS (retention_period = null, value_capture_type = null, exclude_ttl_deletes = null, exclude_insert = null, exclude_update = null, exclude_delete = null, allow_txn_exclusion = null)`},
		},
		{
			name: "create change stream with all options",
			from: ``,
			to: `
CREATE CHANGE STREAM SomeStream FOR ALL OPTIONS( retention_period = '7d', value_capture_type = 'NEW_ROW', exclude_ttl_deletes = true, exclude_insert = true, exclude_update = true, exclude_delete = true, allow_txn_exclusion = true );
`,
			ignoreAlterDatabase: true,
			expected:            []string{`CREATE CHANGE STREAM SomeStream FOR ALL OPTIONS (retention_period = "7d", value_capture_type = "NEW_ROW", exclude_ttl_deletes = true, exclude_insert = true, exclude_update = true, exclude_delete = true, allow_txn_exclusion = true)`},
		},
		{
			name: "alter change stream add all options",
			from: `
CREATE CHANGE STREAM SomeStream FOR ALL;
`,
			to: `
CREATE CHANGE STREAM SomeStream FOR ALL OPTIONS( retention_period = '7d', value_capture_type = 'NEW_ROW', exclude_ttl_deletes = true, exclude_insert = true, exclude_update = true, exclude_delete = true, allow_txn_exclusion = true );
`,
			ignoreAlterDatabase: true,
			expected:            []string{`ALTER CHANGE STREAM SomeStream SET OPTIONS (retention_period = "7d", value_capture_type = "NEW_ROW", exclude_ttl_deletes = true, exclude_insert = true, exclude_update = true, exclude_delete = true, allow_txn_exclusion = true)`},
		},
		{
			name: "both sides have identical fields of timestamp with a default value",
			from: `
CREATE TABLE Nonces (
  nonce INT64 NOT NULL,
  expires TIMESTAMP NOT NULL DEFAULT(TIMESTAMP '2000-01-01 00:00:00.000000+00:00'),
) PRIMARY KEY(nonce);
`,
			to: `
CREATE TABLE Nonces (
  nonce INT64 NOT NULL,
  expires TIMESTAMP NOT NULL DEFAULT(TIMESTAMP '2000-01-01 12:00:00.000000+00:00'),
) PRIMARY KEY(nonce);
`,
			ignoreAlterDatabase: true,
			expected: []string{
				`ALTER TABLE Nonces ALTER COLUMN expires TIMESTAMP NOT NULL DEFAULT (TIMESTAMP "2000-01-01 12:00:00.000000+00:00")`,
			},
		},
		{
			name: "change stream FOR tables order does not matter",
			from: `
CREATE TABLE t1 (
  id INT64 NOT NULL,
) PRIMARY KEY(id);
CREATE TABLE t2 (
  id INT64 NOT NULL,
) PRIMARY KEY(id);
CREATE CHANGE STREAM cs FOR t1, t2;
`,
			to: `
CREATE TABLE t1 (
  id INT64 NOT NULL,
) PRIMARY KEY(id);
CREATE TABLE t2 (
  id INT64 NOT NULL,
) PRIMARY KEY(id);
CREATE CHANGE STREAM cs FOR t2, t1;
`,
			expected: []string{},
		},
		{
			name: "change stream FOR table column order does not matter",
			from: `
CREATE TABLE t1 (
  id INT64 NOT NULL,
  a INT64,
  b INT64,
) PRIMARY KEY(id);
CREATE CHANGE STREAM cs FOR t1(a, b);
`,
			to: `
CREATE TABLE t1 (
  id INT64 NOT NULL,
  a INT64,
  b INT64,
) PRIMARY KEY(id);
CREATE CHANGE STREAM cs FOR t1(b, a);
`,
			expected: []string{},
		},
		{
			name: "change stream FOR tables change is still detected",
			from: `
CREATE TABLE t1 (
  id INT64 NOT NULL,
) PRIMARY KEY(id);
CREATE TABLE t2 (
  id INT64 NOT NULL,
) PRIMARY KEY(id);
CREATE TABLE t3 (
  id INT64 NOT NULL,
) PRIMARY KEY(id);
CREATE CHANGE STREAM cs FOR t1, t2;
`,
			to: `
CREATE TABLE t1 (
  id INT64 NOT NULL,
) PRIMARY KEY(id);
CREATE TABLE t2 (
  id INT64 NOT NULL,
) PRIMARY KEY(id);
CREATE TABLE t3 (
  id INT64 NOT NULL,
) PRIMARY KEY(id);
CREATE CHANGE STREAM cs FOR t1, t3;
`,
			expected: []string{
				"ALTER CHANGE STREAM cs SET FOR t1, t3",
			},
		},
		{
			name: "change stream OPTIONS order does not matter",
			from: `
CREATE TABLE t1 (
  id INT64 NOT NULL,
) PRIMARY KEY(id);
CREATE CHANGE STREAM cs FOR t1 OPTIONS (retention_period = '7d', value_capture_type = 'NEW_VALUES');
`,
			to: `
CREATE TABLE t1 (
  id INT64 NOT NULL,
) PRIMARY KEY(id);
CREATE CHANGE STREAM cs FOR t1 OPTIONS (value_capture_type = 'NEW_VALUES', retention_period = '7d');
`,
			expected: []string{},
		},
		{
			name: "change stream OPTIONS value change is still detected",
			from: `
CREATE TABLE t1 (
  id INT64 NOT NULL,
) PRIMARY KEY(id);
CREATE CHANGE STREAM cs FOR t1 OPTIONS (retention_period = '7d', value_capture_type = 'NEW_VALUES');
`,
			to: `
CREATE TABLE t1 (
  id INT64 NOT NULL,
) PRIMARY KEY(id);
CREATE CHANGE STREAM cs FOR t1 OPTIONS (retention_period = '14d', value_capture_type = 'NEW_VALUES');
`,
			expected: []string{
				`ALTER CHANGE STREAM cs SET OPTIONS (retention_period = "14d", value_capture_type = "NEW_VALUES")`,
			},
		},
		{
			name: "recreating table and change stream watches multiple tables",
			from: `
				CREATE TABLE T1 (
  id INT64,
  name STRING(100),
) PRIMARY KEY(id, name);
				CREATE TABLE T2 (id INT64 PRIMARY KEY);
				CREATE CHANGE STREAM CS1 FOR T1, T2;
			`,
			to: `
				CREATE TABLE T1 (
  id INT64,
  name STRING(100) NOT NULL,
) PRIMARY KEY(id, name);
				CREATE TABLE T2 (id INT64 PRIMARY KEY);
				CREATE CHANGE STREAM CS1 FOR T1, T2;
			`,
			expected: []string{
				`ALTER CHANGE STREAM CS1 SET FOR T2`,
				`DROP TABLE T1`,
				`
CREATE TABLE T1 (
  id INT64,
  name STRING(100) NOT NULL
) PRIMARY KEY (id, name)`,
				`ALTER CHANGE STREAM CS1 SET FOR T1, T2`,
			},
		},
		{
			name: "recreating table and only change stream is for that table",
			from: `
				CREATE TABLE T1 (
  id INT64,
  name STRING(100),
) PRIMARY KEY(id, name);
				CREATE CHANGE STREAM CS1 FOR T1;
			`,
			to: `
				CREATE TABLE T1 (
  id INT64,
  name STRING(100) NOT NULL,
) PRIMARY KEY(id, name);
				CREATE CHANGE STREAM CS1 FOR T1;
			`,
			expected: []string{
				`DROP CHANGE STREAM CS1`,
				`DROP TABLE T1`,
				`
CREATE TABLE T1 (
  id INT64,
  name STRING(100) NOT NULL
) PRIMARY KEY (id, name)`,
				`CREATE CHANGE STREAM CS1 FOR T1`,
			},
		},
		{
			name: "drop change stream referenced by multiple tables",
			from: `
CREATE TABLE t1 (
  t1_1 INT64 NOT NULL,
) PRIMARY KEY(t1_1);
CREATE TABLE t2 (
  t2_1 INT64 NOT NULL,
) PRIMARY KEY(t2_1);
CREATE CHANGE STREAM SomeStream FOR t1, t2;
`,
			to: `
CREATE TABLE t1 (
  t1_1 INT64 NOT NULL,
) PRIMARY KEY(t1_1);
CREATE TABLE t2 (
  t2_1 INT64 NOT NULL,
) PRIMARY KEY(t2_1);
`,
			ignoreAlterDatabase: true,
			expected:            []string{"DROP CHANGE STREAM SomeStream"},
		},
		{
			name: "repoint change stream before dropping old table",
			from: `
CREATE TABLE t1 (
  t1_1 INT64 NOT NULL,
) PRIMARY KEY(t1_1);
CREATE TABLE t2 (
  t2_1 INT64 NOT NULL,
) PRIMARY KEY(t2_1);
CREATE CHANGE STREAM SomeStream FOR t1;
`,
			to: `
CREATE TABLE t2 (
  t2_1 INT64 NOT NULL,
) PRIMARY KEY(t2_1);
CREATE CHANGE STREAM SomeStream FOR t2;
`,
			ignoreAlterDatabase: true,
			expected: []string{
				"ALTER CHANGE STREAM SomeStream SET FOR t2",
				"DROP TABLE t1",
			},
		},
		{
			name: "drop table with change stream and grant",
			from: `
CREATE TABLE t1 (
  id INT64 NOT NULL,
) PRIMARY KEY(id);
CREATE ROLE role1;
CREATE CHANGE STREAM CS1 FOR t1;
GRANT SELECT ON CHANGE STREAM CS1 TO ROLE role1;
`,
			to: `
CREATE ROLE role1;
`,
			ignoreAlterDatabase: true,
			expected: []string{
				"DROP CHANGE STREAM CS1",
				"DROP TABLE t1",
			},
		},
		{
			name: "avoid duplicate alter change stream set for on drop table",
			from: `
CREATE TABLE t1 (
  id INT64 NOT NULL,
) PRIMARY KEY(id);
CREATE TABLE t2 (
  id INT64 NOT NULL,
) PRIMARY KEY(id);
CREATE CHANGE STREAM SomeStream FOR t1, t2;
`,
			to: `
CREATE TABLE t2 (
  id INT64 NOT NULL,
) PRIMARY KEY(id);
CREATE CHANGE STREAM SomeStream FOR t2;
`,
			ignoreAlterDatabase: true,
			expected: []string{
				"ALTER CHANGE STREAM SomeStream SET FOR t2",
				"DROP TABLE t1",
			},
		},
		{
			name: "drop change stream when dropping table with remaining tables",
			from: `
CREATE TABLE t1 (
  id INT64 NOT NULL,
) PRIMARY KEY(id);
CREATE TABLE t2 (
  id INT64 NOT NULL,
) PRIMARY KEY(id);
CREATE CHANGE STREAM SomeStream FOR t1, t2;
`,
			to: `
CREATE TABLE t2 (
  id INT64 NOT NULL,
) PRIMARY KEY(id);
`,
			ignoreAlterDatabase: true,
			expected: []string{
				"DROP CHANGE STREAM SomeStream",
				"DROP TABLE t1",
			},
		},
		{
			name: "drop two of three tables with multi-table change stream",
			from: `
CREATE TABLE t1 (
  id INT64 NOT NULL,
) PRIMARY KEY(id);
CREATE TABLE t2 (
  id INT64 NOT NULL,
) PRIMARY KEY(id);
CREATE TABLE t3 (
  id INT64 NOT NULL,
) PRIMARY KEY(id);
CREATE CHANGE STREAM CS1 FOR t1, t2, t3;
`,
			to: `
CREATE TABLE t1 (
  id INT64 NOT NULL,
) PRIMARY KEY(id);
CREATE CHANGE STREAM CS1 FOR t1;
`,
			ignoreAlterDatabase: true,
			expected: []string{
				"ALTER CHANGE STREAM CS1 SET FOR t1",
				"DROP TABLE t2",
				"DROP TABLE t3",
			},
		},
		{
			name: "change stream from table to FOR ALL with table drop",
			from: `
CREATE TABLE t1 (
  id INT64 NOT NULL,
) PRIMARY KEY(id);
CREATE TABLE t2 (
  id INT64 NOT NULL,
) PRIMARY KEY(id);
CREATE CHANGE STREAM CS1 FOR t1;
`,
			to: `
CREATE TABLE t2 (
  id INT64 NOT NULL,
) PRIMARY KEY(id);
CREATE CHANGE STREAM CS1 FOR ALL;
`,
			ignoreAlterDatabase: true,
			expected: []string{
				"ALTER CHANGE STREAM CS1 SET FOR ALL",
				"DROP TABLE t1",
			},
		},
		{
			name: "change stream from multi-table to FOR ALL with table drop",
			from: `
CREATE TABLE t1 (
  id INT64 NOT NULL,
) PRIMARY KEY(id);
CREATE TABLE t2 (
  id INT64 NOT NULL,
) PRIMARY KEY(id);
CREATE CHANGE STREAM CS1 FOR t1, t2;
`,
			to: `
CREATE TABLE t2 (
  id INT64 NOT NULL,
) PRIMARY KEY(id);
CREATE CHANGE STREAM CS1 FOR ALL;
`,
			ignoreAlterDatabase: true,
			expected: []string{
				"ALTER CHANGE STREAM CS1 SET FOR ALL",
				"DROP TABLE t1",
			},
		},
		{
			name: "change stream from multi-table to none with table drop",
			from: `
CREATE TABLE t1 (
  id INT64 NOT NULL,
) PRIMARY KEY(id);
CREATE TABLE t2 (
  id INT64 NOT NULL,
) PRIMARY KEY(id);
CREATE CHANGE STREAM CS1 FOR t1, t2;
`,
			to: `
CREATE TABLE t2 (
  id INT64 NOT NULL,
) PRIMARY KEY(id);
CREATE CHANGE STREAM CS1;
`,
			ignoreAlterDatabase: true,
			expected: []string{
				"ALTER CHANGE STREAM CS1 DROP FOR ALL",
				"DROP TABLE t1",
			},
		},
		{
			name: "multiple change streams watching same table",
			from: `
CREATE TABLE t1 (
  id INT64 NOT NULL,
) PRIMARY KEY(id);
CREATE CHANGE STREAM CS1 FOR t1;
CREATE CHANGE STREAM CS2 FOR t1;
`,
			to: `
CREATE TABLE t1 (
  id INT64 NOT NULL,
) PRIMARY KEY(id);
CREATE CHANGE STREAM CS1 FOR t1;
`,
			ignoreAlterDatabase: true,
			expected: []string{
				"DROP CHANGE STREAM CS2",
			},
		},
		{
			name: "overlapping change streams on multiple tables",
			from: `
CREATE TABLE t1 (
  id INT64 NOT NULL,
) PRIMARY KEY(id);
CREATE TABLE t2 (
  id INT64 NOT NULL,
) PRIMARY KEY(id);
CREATE CHANGE STREAM CS1 FOR t1, t2;
CREATE CHANGE STREAM CS2 FOR t2;
`,
			to: `
CREATE TABLE t2 (
  id INT64 NOT NULL,
) PRIMARY KEY(id);
CREATE CHANGE STREAM CS1 FOR t2;
CREATE CHANGE STREAM CS2 FOR t2;
`,
			ignoreAlterDatabase: true,
			expected: []string{
				"ALTER CHANGE STREAM CS1 SET FOR t2",
				"DROP TABLE t1",
			},
		},
		{
			name: "change stream FOR with empty column list on one table",
			from: `
CREATE TABLE t1 (
  id INT64 NOT NULL,
  a INT64,
) PRIMARY KEY(id);
CREATE TABLE t2 (
  id INT64 NOT NULL,
) PRIMARY KEY(id);
CREATE CHANGE STREAM cs FOR t1(a), t2();
`,
			to: `
CREATE TABLE t1 (
  id INT64 NOT NULL,
  a INT64,
) PRIMARY KEY(id);
CREATE TABLE t2 (
  id INT64 NOT NULL,
) PRIMARY KEY(id);
CREATE CHANGE STREAM cs FOR t1(a), t2();
`,
			expected: []string{},
		},
		{
			name: "change stream composite: create, alter FOR, alter OPTIONS, drop in single diff",
			from: `
CREATE TABLE t1 (
  id INT64 NOT NULL,
) PRIMARY KEY(id);
CREATE TABLE t2 (
  id INT64 NOT NULL,
) PRIMARY KEY(id);
CREATE TABLE t3 (
  id INT64 NOT NULL,
) PRIMARY KEY(id);
CREATE CHANGE STREAM cs_zebra_for_change FOR t1 OPTIONS (retention_period = '7d');
CREATE CHANGE STREAM cs_alpha_options_change FOR t1 OPTIONS (retention_period = '7d');
CREATE CHANGE STREAM cs_mike_to_drop FOR t1 OPTIONS (retention_period = '7d');
CREATE CHANGE STREAM cs_unchanged FOR t1 OPTIONS (retention_period = '7d');
`,
			to: `
CREATE TABLE t1 (
  id INT64 NOT NULL,
) PRIMARY KEY(id);
CREATE TABLE t2 (
  id INT64 NOT NULL,
) PRIMARY KEY(id);
CREATE TABLE t3 (
  id INT64 NOT NULL,
) PRIMARY KEY(id);
CREATE CHANGE STREAM cs_zebra_for_change FOR t2 OPTIONS (retention_period = '7d');
CREATE CHANGE STREAM cs_alpha_options_change FOR t1 OPTIONS (retention_period = '14d');
CREATE CHANGE STREAM cs_papa_to_create FOR t3 OPTIONS (retention_period = '7d');
CREATE CHANGE STREAM cs_unchanged FOR t1 OPTIONS (retention_period = '7d');
`,
			expected: []string{
				`ALTER CHANGE STREAM cs_alpha_options_change SET OPTIONS (retention_period = "14d")`,
				"ALTER CHANGE STREAM cs_zebra_for_change SET FOR t2",
				`CREATE CHANGE STREAM cs_papa_to_create FOR t3 OPTIONS (retention_period = "7d")`,
				"DROP CHANGE STREAM cs_mike_to_drop",
			},
		},
	})
}
