package hammer_test

import "testing"

func TestDiffSequence(t *testing.T) {
	runDiffTests(t, []diffTestCase{
		{
			name: "create sequence",
			from: ``,
			to: `
CREATE SEQUENCE seq1 OPTIONS (sequence_kind = 'bit_reversed_positive');
`,
			expected: []string{
				`CREATE SEQUENCE seq1 OPTIONS (sequence_kind = "bit_reversed_positive")`,
			},
		},
		{
			name: "drop sequence",
			from: `
CREATE SEQUENCE seq1 OPTIONS (sequence_kind = 'bit_reversed_positive');
`,
			to: ``,
			expected: []string{
				`DROP SEQUENCE seq1`,
			},
		},
		{
			name: "same sequence",
			from: `
CREATE SEQUENCE seq1 OPTIONS (sequence_kind = 'bit_reversed_positive', skip_range_min = 1, skip_range_max = 1000);
`,
			to: `
CREATE SEQUENCE IF NOT EXISTS seq1 OPTIONS (skip_range_max = 1000, skip_range_min = 1, sequence_kind = 'bit_reversed_positive');
`,
			expected: []string{},
		},
		{
			name: "clause form and options form are equivalent",
			from: `
CREATE SEQUENCE seq1 OPTIONS (sequence_kind = 'bit_reversed_positive', skip_range_min = 1, skip_range_max = 1000, start_with_counter = 50);
`,
			to: `
CREATE SEQUENCE seq1 BIT_REVERSED_POSITIVE SKIP RANGE 1, 1000 START COUNTER WITH 50 OPTIONS (sequence_kind = 'bit_reversed_positive');
`,
			expected: []string{},
		},
		{
			name: "create sequence written in clause form",
			from: ``,
			to: `
CREATE SEQUENCE seq1 BIT_REVERSED_POSITIVE SKIP RANGE 1, 1000 START COUNTER WITH 50 OPTIONS (sequence_kind = 'bit_reversed_positive');
`,
			expected: []string{
				`CREATE SEQUENCE seq1 OPTIONS (sequence_kind = "bit_reversed_positive", skip_range_min = 1, skip_range_max = 1000, start_with_counter = 50)`,
			},
		},
		{
			name: "alter sequence options",
			from: `
CREATE SEQUENCE seq1 OPTIONS (sequence_kind = 'bit_reversed_positive', skip_range_min = 1, skip_range_max = 1000);
`,
			to: `
CREATE SEQUENCE seq1 OPTIONS (sequence_kind = 'bit_reversed_positive', skip_range_min = 1, skip_range_max = 5000, start_with_counter = 100);
`,
			expected: []string{
				`ALTER SEQUENCE seq1 SET OPTIONS (skip_range_max = 5000, start_with_counter = 100)`,
			},
		},
		{
			name: "alter sequence removes skip range",
			from: `
CREATE SEQUENCE seq1 BIT_REVERSED_POSITIVE SKIP RANGE 1, 1000 OPTIONS (sequence_kind = 'bit_reversed_positive');
`,
			to: `
CREATE SEQUENCE seq1 OPTIONS (sequence_kind = 'bit_reversed_positive');
`,
			expected: []string{
				`ALTER SEQUENCE seq1 SET OPTIONS (skip_range_min = null, skip_range_max = null)`,
			},
		},
		{
			name: "removing start_with_counter does not restart the counter",
			from: `
CREATE SEQUENCE seq1 OPTIONS (sequence_kind = 'bit_reversed_positive', start_with_counter = 100);
`,
			to: `
CREATE SEQUENCE seq1 OPTIONS (sequence_kind = 'bit_reversed_positive');
`,
			expected: []string{},
		},
		{
			name: "removing sequence_kind keeps the existing kind",
			from: `
CREATE SEQUENCE seq1 OPTIONS (sequence_kind = 'bit_reversed_positive', skip_range_min = 1, skip_range_max = 1000);
`,
			to: `
CREATE SEQUENCE seq1 OPTIONS (skip_range_min = 1, skip_range_max = 1000);
`,
			expected: []string{},
		},
		{
			name: "change sequence_kind",
			from: `
CREATE SEQUENCE seq1 OPTIONS (sequence_kind = 'bit_reversed_positive');
`,
			to: `
CREATE SEQUENCE seq1 OPTIONS (sequence_kind = 'new_kind');
`,
			expected: []string{
				`ALTER SEQUENCE seq1 SET OPTIONS (sequence_kind = "new_kind")`,
			},
		},
		{
			name: "create sequence before table that uses it",
			from: ``,
			to: `
CREATE TABLE t1 (
  id INT64 NOT NULL DEFAULT (GET_NEXT_SEQUENCE_VALUE(SEQUENCE seq1)),
) PRIMARY KEY(id);
CREATE SEQUENCE seq1 OPTIONS (sequence_kind = 'bit_reversed_positive');
`,
			expected: []string{
				`CREATE SEQUENCE seq1 OPTIONS (sequence_kind = "bit_reversed_positive")`,
				`
CREATE TABLE t1 (
  id INT64 NOT NULL DEFAULT (GET_NEXT_SEQUENCE_VALUE(SEQUENCE seq1))
) PRIMARY KEY (id)`,
			},
		},
		{
			name: "drop sequence after table that uses it",
			from: `
CREATE SEQUENCE seq1 OPTIONS (sequence_kind = 'bit_reversed_positive');
CREATE TABLE t1 (
  id INT64 NOT NULL DEFAULT (GET_NEXT_SEQUENCE_VALUE(SEQUENCE seq1)),
) PRIMARY KEY(id);
`,
			to: ``,
			expected: []string{
				`DROP TABLE t1`,
				`DROP SEQUENCE seq1`,
			},
		},
		{
			name: "drop sequence after column that uses it",
			from: `
CREATE SEQUENCE seq1 OPTIONS (sequence_kind = 'bit_reversed_positive');
CREATE TABLE t1 (
  id INT64 NOT NULL,
  num INT64 DEFAULT (GET_NEXT_SEQUENCE_VALUE(SEQUENCE seq1)),
) PRIMARY KEY(id);
`,
			to: `
CREATE TABLE t1 (
  id INT64 NOT NULL,
) PRIMARY KEY(id);
`,
			expected: []string{
				`ALTER TABLE t1 DROP COLUMN num`,
				`DROP SEQUENCE seq1`,
			},
		},
		{
			name: "switch column default to another sequence",
			from: `
CREATE SEQUENCE seq1 OPTIONS (sequence_kind = 'bit_reversed_positive');
CREATE TABLE t1 (
  id INT64 NOT NULL,
  num INT64 DEFAULT (GET_NEXT_SEQUENCE_VALUE(SEQUENCE seq1)),
) PRIMARY KEY(id);
`,
			to: `
CREATE SEQUENCE seq2 OPTIONS (sequence_kind = 'bit_reversed_positive');
CREATE TABLE t1 (
  id INT64 NOT NULL,
  num INT64 DEFAULT (GET_NEXT_SEQUENCE_VALUE(SEQUENCE seq2)),
) PRIMARY KEY(id);
`,
			expected: []string{
				`CREATE SEQUENCE seq2 OPTIONS (sequence_kind = "bit_reversed_positive")`,
				`ALTER TABLE t1 ALTER COLUMN num INT64 DEFAULT (GET_NEXT_SEQUENCE_VALUE(SEQUENCE seq2))`,
				`DROP SEQUENCE seq1`,
			},
		},
		{
			name: "schema-qualified sequence",
			from: ``,
			to: `
CREATE SCHEMA sch1;
CREATE SEQUENCE sch1.seq1 OPTIONS (sequence_kind = 'bit_reversed_positive');
`,
			expected: []string{
				`CREATE SCHEMA sch1`,
				`CREATE SEQUENCE sch1.seq1 OPTIONS (sequence_kind = "bit_reversed_positive")`,
			},
		},
		{
			name: "drop schema after its qualified sequence",
			from: `
CREATE SCHEMA sch1;
CREATE SEQUENCE sch1.seq1 OPTIONS (sequence_kind = 'bit_reversed_positive');
`,
			to: ``,
			expected: []string{
				`DROP SEQUENCE sch1.seq1`,
				`DROP SCHEMA sch1`,
			},
		},
		{
			name: "sequence names are case-insensitive",
			from: `
CREATE SEQUENCE seq1 OPTIONS (sequence_kind = 'bit_reversed_positive');
CREATE ROLE role1;
GRANT SELECT ON SEQUENCE seq1 TO ROLE role1;
`,
			to: `
CREATE SEQUENCE Seq1 OPTIONS (sequence_kind = 'bit_reversed_positive');
CREATE ROLE role1;
`,
			expected: []string{
				`REVOKE SELECT ON SEQUENCE seq1 FROM ROLE role1`,
			},
		},
		{
			name: "change grant on all sequences in schema",
			from: `
CREATE SCHEMA sch1;
CREATE ROLE role1;
GRANT SELECT ON ALL SEQUENCES IN SCHEMA sch1 TO ROLE role1;
`,
			to: `
CREATE SCHEMA sch1;
CREATE ROLE role1;
GRANT SELECT, UPDATE ON ALL SEQUENCES IN SCHEMA sch1 TO ROLE role1;
`,
			expected: []string{
				`REVOKE SELECT ON ALL SEQUENCES IN SCHEMA sch1 FROM ROLE role1`,
				`GRANT SELECT, UPDATE ON ALL SEQUENCES IN SCHEMA sch1 TO ROLE role1`,
			},
		},
		{
			name: "same grant on sequence",
			from: `
CREATE SEQUENCE seq1 OPTIONS (sequence_kind = 'bit_reversed_positive');
CREATE ROLE role1;
GRANT SELECT, UPDATE ON SEQUENCE seq1 TO ROLE role1;
`,
			to: `
CREATE SEQUENCE seq1 OPTIONS (sequence_kind = 'bit_reversed_positive');
CREATE ROLE role1;
GRANT SELECT, UPDATE ON SEQUENCE seq1 TO ROLE role1;
`,
			expected: []string{},
		},
		{
			name: "grant on sequence after creating it",
			from: `
CREATE ROLE role1;
`,
			to: `
CREATE SEQUENCE seq1 OPTIONS (sequence_kind = 'bit_reversed_positive');
CREATE ROLE role1;
GRANT SELECT ON SEQUENCE seq1 TO ROLE role1;
`,
			expected: []string{
				`CREATE SEQUENCE seq1 OPTIONS (sequence_kind = "bit_reversed_positive")`,
				`GRANT SELECT ON SEQUENCE seq1 TO ROLE role1`,
			},
		},
		{
			name: "drop sequence without revoking its grant",
			from: `
CREATE SEQUENCE seq1 OPTIONS (sequence_kind = 'bit_reversed_positive');
CREATE ROLE role1;
GRANT SELECT ON SEQUENCE seq1 TO ROLE role1;
`,
			to: `
CREATE ROLE role1;
`,
			expected: []string{
				`DROP SEQUENCE seq1`,
			},
		},
		{
			name: "revoke grant on sequence before dropping role",
			from: `
CREATE SEQUENCE seq1 OPTIONS (sequence_kind = 'bit_reversed_positive');
CREATE ROLE role1;
GRANT SELECT ON SEQUENCE seq1 TO ROLE role1;
`,
			to: `
CREATE SEQUENCE seq1 OPTIONS (sequence_kind = 'bit_reversed_positive');
`,
			expected: []string{
				`REVOKE SELECT ON SEQUENCE seq1 FROM ROLE role1`,
				`DROP ROLE role1`,
			},
		},
	})
}
