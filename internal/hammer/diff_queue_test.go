package hammer_test

import "testing"

func TestDiffQueue(t *testing.T) {
	runDiffTests(t, []diffTestCase{
		{
			name: "create queue",
			from: ``,
			to: `
CREATE QUEUE Q (
  Id INT64 NOT NULL,
  Payload BYTES(MAX) NOT NULL,
) PRIMARY KEY (Id);
`,
			expected: []string{
				`
CREATE QUEUE Q (
  Id INT64 NOT NULL,
  Payload BYTES(MAX) NOT NULL
) PRIMARY KEY (Id)`,
			},
		},
		{
			name: "create table and interleaved queue",
			from: ``,
			to: `
CREATE QUEUE Q (
  UserId INT64 NOT NULL,
  Payload JSON NOT NULL,
) PRIMARY KEY (UserId), INTERLEAVE IN PARENT Users ON DELETE CASCADE;

CREATE TABLE Users (
  UserId INT64 NOT NULL,
) PRIMARY KEY (UserId);
`,
			expected: []string{
				`
CREATE TABLE Users (
  UserId INT64 NOT NULL
) PRIMARY KEY (UserId)`,
				`
CREATE QUEUE Q (
  UserId INT64 NOT NULL,
  Payload JSON NOT NULL
) PRIMARY KEY (UserId),
  INTERLEAVE IN PARENT Users ON DELETE CASCADE`,
			},
		},
		{
			name: "drop queue",
			from: `
CREATE QUEUE Q (
  Id INT64 NOT NULL,
  Payload BYTES(MAX) NOT NULL,
) PRIMARY KEY (Id);
`,
			to: ``,
			expected: []string{
				`DROP QUEUE Q`,
			},
		},
		{
			name: "queue without changes",
			from: `
CREATE QUEUE Q (
  Id INT64 NOT NULL,
  Payload BYTES(MAX) NOT NULL,
) PRIMARY KEY (Id ASC), OPTIONS (receive_mode = 'PULL');
`,
			to: `
create queue Q (
  Id INT64 NOT NULL,
  Payload BYTES(MAX) NOT NULL,
) primary key (Id), options (receive_mode = 'PULL');
`,
			expected: []string{},
		},
		{
			name: "change queue payload type recreates the queue",
			from: `
CREATE QUEUE Q (
  Id INT64 NOT NULL,
  Payload BYTES(MAX) NOT NULL,
) PRIMARY KEY (Id);
`,
			to: `
CREATE QUEUE Q (
  Id INT64 NOT NULL,
  Payload JSON NOT NULL,
) PRIMARY KEY (Id);
`,
			expected: []string{
				`DROP QUEUE Q`,
				`
CREATE QUEUE Q (
  Id INT64 NOT NULL,
  Payload JSON NOT NULL
) PRIMARY KEY (Id)`,
			},
		},
		{
			name: "add queue key column recreates the queue",
			from: `
CREATE QUEUE Q (
  Id INT64 NOT NULL,
  Payload BYTES(MAX) NOT NULL,
) PRIMARY KEY (Id);
`,
			to: `
CREATE QUEUE Q (
  Id INT64 NOT NULL,
  SubId INT64 NOT NULL,
  Payload BYTES(MAX) NOT NULL,
) PRIMARY KEY (Id, SubId);
`,
			expected: []string{
				`DROP QUEUE Q`,
				`
CREATE QUEUE Q (
  Id INT64 NOT NULL,
  SubId INT64 NOT NULL,
  Payload BYTES(MAX) NOT NULL
) PRIMARY KEY (Id, SubId)`,
			},
		},
		{
			name: "change queue primary key direction recreates the queue",
			from: `
CREATE QUEUE Q (
  Id INT64 NOT NULL,
  Payload BYTES(MAX) NOT NULL,
) PRIMARY KEY (Id);
`,
			to: `
CREATE QUEUE Q (
  Id INT64 NOT NULL,
  Payload BYTES(MAX) NOT NULL,
) PRIMARY KEY (Id DESC);
`,
			expected: []string{
				`DROP QUEUE Q`,
				`
CREATE QUEUE Q (
  Id INT64 NOT NULL,
  Payload BYTES(MAX) NOT NULL
) PRIMARY KEY (Id DESC)`,
			},
		},
		{
			name: "change queue options",
			from: `
CREATE QUEUE Q (
  Id INT64 NOT NULL,
  Payload BYTES(MAX) NOT NULL,
) PRIMARY KEY (Id), OPTIONS (receive_mode = 'PULL', locality_group = 'ssd');
`,
			to: `
CREATE QUEUE Q (
  Id INT64 NOT NULL,
  Payload BYTES(MAX) NOT NULL,
) PRIMARY KEY (Id), OPTIONS (receive_mode = 'PULL', disable_send = true);
`,
			expected: []string{
				`ALTER QUEUE Q SET OPTIONS (receive_mode = "PULL", disable_send = true, locality_group = null)`,
			},
		},
		{
			name: "add queue row deletion policy",
			from: `
CREATE QUEUE Q (
  Id INT64 NOT NULL,
  CreatedAt TIMESTAMP NOT NULL,
  Payload BYTES(MAX) NOT NULL,
) PRIMARY KEY (Id, CreatedAt);
`,
			to: `
CREATE QUEUE Q (
  Id INT64 NOT NULL,
  CreatedAt TIMESTAMP NOT NULL,
  Payload BYTES(MAX) NOT NULL,
) PRIMARY KEY (Id, CreatedAt), ROW DELETION POLICY (OLDER_THAN(CreatedAt, INTERVAL 7 DAY));
`,
			expected: []string{
				`ALTER QUEUE Q ADD ROW DELETION POLICY ( OLDER_THAN ( CreatedAt, INTERVAL 7 DAY ))`,
			},
		},
		{
			name: "replace queue row deletion policy",
			from: `
CREATE QUEUE Q (
  Id INT64 NOT NULL,
  CreatedAt TIMESTAMP NOT NULL,
  Payload BYTES(MAX) NOT NULL,
) PRIMARY KEY (Id, CreatedAt), ROW DELETION POLICY (OLDER_THAN(CreatedAt, INTERVAL 7 DAY));
`,
			to: `
CREATE QUEUE Q (
  Id INT64 NOT NULL,
  CreatedAt TIMESTAMP NOT NULL,
  Payload BYTES(MAX) NOT NULL,
) PRIMARY KEY (Id, CreatedAt), ROW DELETION POLICY (OLDER_THAN(CreatedAt, INTERVAL 1 DAY));
`,
			expected: []string{
				`ALTER QUEUE Q REPLACE ROW DELETION POLICY ( OLDER_THAN ( CreatedAt, INTERVAL 1 DAY ))`,
			},
		},
		{
			name: "drop queue row deletion policy",
			from: `
CREATE QUEUE Q (
  Id INT64 NOT NULL,
  CreatedAt TIMESTAMP NOT NULL,
  Payload BYTES(MAX) NOT NULL,
) PRIMARY KEY (Id, CreatedAt), ROW DELETION POLICY (OLDER_THAN(CreatedAt, INTERVAL 7 DAY));
`,
			to: `
CREATE QUEUE Q (
  Id INT64 NOT NULL,
  CreatedAt TIMESTAMP NOT NULL,
  Payload BYTES(MAX) NOT NULL,
) PRIMARY KEY (Id, CreatedAt);
`,
			expected: []string{
				`ALTER QUEUE Q DROP ROW DELETION POLICY`,
			},
		},
		{
			name: "change queue interleave to IN PARENT with ON DELETE CASCADE in two steps",
			from: `
CREATE TABLE T (Id INT64 NOT NULL) PRIMARY KEY (Id);
CREATE QUEUE Q (
  Id INT64 NOT NULL,
  Payload BYTES(MAX) NOT NULL,
) PRIMARY KEY (Id), INTERLEAVE IN T;
`,
			to: `
CREATE TABLE T (Id INT64 NOT NULL) PRIMARY KEY (Id);
CREATE QUEUE Q (
  Id INT64 NOT NULL,
  Payload BYTES(MAX) NOT NULL,
) PRIMARY KEY (Id), INTERLEAVE IN PARENT T ON DELETE CASCADE;
`,
			expected: []string{
				`ALTER QUEUE Q SET INTERLEAVE IN PARENT T ON DELETE NO ACTION`,
				`ALTER QUEUE Q SET ON DELETE CASCADE`,
			},
		},
		{
			name: "change queue interleave to IN PARENT with ON DELETE NO ACTION",
			from: `
CREATE TABLE T (Id INT64 NOT NULL) PRIMARY KEY (Id);
CREATE QUEUE Q (
  Id INT64 NOT NULL,
  Payload BYTES(MAX) NOT NULL,
) PRIMARY KEY (Id), INTERLEAVE IN T;
`,
			to: `
CREATE TABLE T (Id INT64 NOT NULL) PRIMARY KEY (Id);
CREATE QUEUE Q (
  Id INT64 NOT NULL,
  Payload BYTES(MAX) NOT NULL,
) PRIMARY KEY (Id), INTERLEAVE IN PARENT T;
`,
			expected: []string{
				`ALTER QUEUE Q SET INTERLEAVE IN PARENT T`,
			},
		},
		{
			name: "change queue interleave from IN PARENT to IN",
			from: `
CREATE TABLE T (Id INT64 NOT NULL) PRIMARY KEY (Id);
CREATE QUEUE Q (
  Id INT64 NOT NULL,
  Payload BYTES(MAX) NOT NULL,
) PRIMARY KEY (Id), INTERLEAVE IN PARENT T ON DELETE CASCADE;
`,
			to: `
CREATE TABLE T (Id INT64 NOT NULL) PRIMARY KEY (Id);
CREATE QUEUE Q (
  Id INT64 NOT NULL,
  Payload BYTES(MAX) NOT NULL,
) PRIMARY KEY (Id), INTERLEAVE IN T;
`,
			expected: []string{
				`ALTER QUEUE Q SET INTERLEAVE IN T`,
			},
		},
		{
			name: "change queue ON DELETE action",
			from: `
CREATE TABLE T (Id INT64 NOT NULL) PRIMARY KEY (Id);
CREATE QUEUE Q (
  Id INT64 NOT NULL,
  Payload BYTES(MAX) NOT NULL,
) PRIMARY KEY (Id), INTERLEAVE IN PARENT T;
`,
			to: `
CREATE TABLE T (Id INT64 NOT NULL) PRIMARY KEY (Id);
CREATE QUEUE Q (
  Id INT64 NOT NULL,
  Payload BYTES(MAX) NOT NULL,
) PRIMARY KEY (Id), INTERLEAVE IN PARENT T ON DELETE CASCADE;
`,
			expected: []string{
				`ALTER QUEUE Q SET ON DELETE CASCADE`,
			},
		},
		{
			name: "queue ON DELETE NO ACTION is the default of IN PARENT",
			from: `
CREATE TABLE T (Id INT64 NOT NULL) PRIMARY KEY (Id);
CREATE QUEUE Q (
  Id INT64 NOT NULL,
  Payload BYTES(MAX) NOT NULL,
) PRIMARY KEY (Id), INTERLEAVE IN PARENT T;
`,
			to: `
CREATE TABLE T (Id INT64 NOT NULL) PRIMARY KEY (Id);
CREATE QUEUE Q (
  Id INT64 NOT NULL,
  Payload BYTES(MAX) NOT NULL,
) PRIMARY KEY (Id), INTERLEAVE IN PARENT T ON DELETE NO ACTION;
`,
			expected: []string{},
		},
		{
			name: "change queue parent table recreates the queue",
			from: `
CREATE TABLE T1 (Id INT64 NOT NULL) PRIMARY KEY (Id);
CREATE TABLE T2 (Id INT64 NOT NULL) PRIMARY KEY (Id);
CREATE QUEUE Q (
  Id INT64 NOT NULL,
  Payload BYTES(MAX) NOT NULL,
) PRIMARY KEY (Id), INTERLEAVE IN T1;
`,
			to: `
CREATE TABLE T1 (Id INT64 NOT NULL) PRIMARY KEY (Id);
CREATE TABLE T2 (Id INT64 NOT NULL) PRIMARY KEY (Id);
CREATE QUEUE Q (
  Id INT64 NOT NULL,
  Payload BYTES(MAX) NOT NULL,
) PRIMARY KEY (Id), INTERLEAVE IN T2;
`,
			expected: []string{
				`DROP QUEUE Q`,
				`
CREATE QUEUE Q (
  Id INT64 NOT NULL,
  Payload BYTES(MAX) NOT NULL
) PRIMARY KEY (Id),
  INTERLEAVE IN T2`,
			},
		},
		{
			name: "queue parent table name is not case-sensitive",
			from: `
CREATE TABLE T (Id INT64 NOT NULL) PRIMARY KEY (Id);
CREATE QUEUE Q (
  Id INT64 NOT NULL,
  Payload BYTES(MAX) NOT NULL,
) PRIMARY KEY (Id), INTERLEAVE IN PARENT T;
`,
			to: `
CREATE TABLE t (Id INT64 NOT NULL) PRIMARY KEY (Id);
CREATE QUEUE Q (
  Id INT64 NOT NULL,
  Payload BYTES(MAX) NOT NULL,
) PRIMARY KEY (Id), INTERLEAVE IN PARENT t;
`,
			expected: []string{},
		},
		{
			name: "interleave existing queue recreates the queue",
			from: `
CREATE TABLE T (Id INT64 NOT NULL) PRIMARY KEY (Id);
CREATE QUEUE Q (
  Id INT64 NOT NULL,
  Payload BYTES(MAX) NOT NULL,
) PRIMARY KEY (Id);
`,
			to: `
CREATE TABLE T (Id INT64 NOT NULL) PRIMARY KEY (Id);
CREATE QUEUE Q (
  Id INT64 NOT NULL,
  Payload BYTES(MAX) NOT NULL,
) PRIMARY KEY (Id), INTERLEAVE IN T;
`,
			expected: []string{
				`DROP QUEUE Q`,
				`
CREATE QUEUE Q (
  Id INT64 NOT NULL,
  Payload BYTES(MAX) NOT NULL
) PRIMARY KEY (Id),
  INTERLEAVE IN T`,
			},
		},
		{
			name: "recreate parent table recreates the interleaved queue",
			from: `
CREATE TABLE T (Id INT64 NOT NULL) PRIMARY KEY (Id);
CREATE QUEUE Q (
  Id INT64 NOT NULL,
  Payload BYTES(MAX) NOT NULL,
) PRIMARY KEY (Id), INTERLEAVE IN PARENT T;
`,
			to: `
CREATE TABLE T (Id INT64 NOT NULL) PRIMARY KEY (Id DESC);
CREATE QUEUE Q (
  Id INT64 NOT NULL,
  Payload BYTES(MAX) NOT NULL,
) PRIMARY KEY (Id), INTERLEAVE IN PARENT T;
`,
			expected: []string{
				`DROP QUEUE Q`,
				`DROP TABLE T`,
				`
CREATE TABLE T (
  Id INT64 NOT NULL
) PRIMARY KEY (Id DESC)`,
				`
CREATE QUEUE Q (
  Id INT64 NOT NULL,
  Payload BYTES(MAX) NOT NULL
) PRIMARY KEY (Id),
  INTERLEAVE IN PARENT T`,
			},
		},
		{
			name: "drop parent table and interleaved queue",
			from: `
CREATE TABLE T (Id INT64 NOT NULL) PRIMARY KEY (Id);
CREATE QUEUE Q (
  Id INT64 NOT NULL,
  Payload BYTES(MAX) NOT NULL,
) PRIMARY KEY (Id), INTERLEAVE IN PARENT T;
`,
			to: ``,
			expected: []string{
				`DROP QUEUE Q`,
				`DROP TABLE T`,
			},
		},
		{
			name: "grant on queue",
			from: `
CREATE ROLE R;
CREATE QUEUE Q (Id INT64 NOT NULL, Payload BYTES(MAX) NOT NULL) PRIMARY KEY (Id);
`,
			to: `
CREATE ROLE R;
CREATE QUEUE Q (Id INT64 NOT NULL, Payload BYTES(MAX) NOT NULL) PRIMARY KEY (Id);
GRANT SELECT, INSERT ON QUEUE Q TO ROLE R;
`,
			expected: []string{
				`GRANT SELECT, INSERT ON QUEUE Q TO ROLE R`,
			},
		},
		{
			name: "grant on queue without changes",
			from: `
CREATE ROLE R;
CREATE QUEUE Q (Id INT64 NOT NULL, Payload BYTES(MAX) NOT NULL) PRIMARY KEY (Id);
GRANT SELECT ON QUEUE Q TO ROLE R;
`,
			to: `
CREATE ROLE R;
CREATE QUEUE Q (Id INT64 NOT NULL, Payload BYTES(MAX) NOT NULL) PRIMARY KEY (Id);
GRANT SELECT ON QUEUE Q TO ROLE R;
`,
			expected: []string{},
		},
		{
			name: "change grant privileges on queue",
			from: `
CREATE ROLE R;
CREATE QUEUE Q (Id INT64 NOT NULL, Payload BYTES(MAX) NOT NULL) PRIMARY KEY (Id);
GRANT SELECT ON QUEUE Q TO ROLE R;
`,
			to: `
CREATE ROLE R;
CREATE QUEUE Q (Id INT64 NOT NULL, Payload BYTES(MAX) NOT NULL) PRIMARY KEY (Id);
GRANT INSERT ON QUEUE Q TO ROLE R;
`,
			expected: []string{
				`REVOKE SELECT ON QUEUE Q FROM ROLE R`,
				`GRANT INSERT ON QUEUE Q TO ROLE R`,
			},
		},
		{
			name: "drop queue with grant",
			from: `
CREATE ROLE R;
CREATE QUEUE Q (Id INT64 NOT NULL, Payload BYTES(MAX) NOT NULL) PRIMARY KEY (Id);
GRANT SELECT ON QUEUE Q TO ROLE R;
`,
			to: `
CREATE ROLE R;
`,
			expected: []string{
				`DROP QUEUE Q`,
			},
		},
		{
			name: "recreate queue with grant",
			from: `
CREATE ROLE R;
CREATE QUEUE Q (Id INT64 NOT NULL, Payload BYTES(MAX) NOT NULL) PRIMARY KEY (Id);
GRANT SELECT ON QUEUE Q TO ROLE R;
`,
			to: `
CREATE ROLE R;
CREATE QUEUE Q (Id INT64 NOT NULL, Payload JSON NOT NULL) PRIMARY KEY (Id);
GRANT SELECT ON QUEUE Q TO ROLE R;
`,
			expected: []string{
				`DROP QUEUE Q`,
				"CREATE QUEUE Q (\n  Id INT64 NOT NULL,\n  Payload JSON NOT NULL\n) PRIMARY KEY (Id)",
				`GRANT SELECT ON QUEUE Q TO ROLE R`,
			},
		},
		{
			name: "recreate parent table regrants the interleaved queue",
			from: `
CREATE ROLE R;
CREATE TABLE T (Id INT64 NOT NULL) PRIMARY KEY (Id);
CREATE QUEUE Q (Id INT64 NOT NULL, Payload BYTES(MAX) NOT NULL) PRIMARY KEY (Id), INTERLEAVE IN PARENT T;
GRANT SELECT ON QUEUE Q TO ROLE R;
`,
			to: `
CREATE ROLE R;
CREATE TABLE T (Id STRING(36) NOT NULL) PRIMARY KEY (Id);
CREATE QUEUE Q (Id STRING(36) NOT NULL, Payload BYTES(MAX) NOT NULL) PRIMARY KEY (Id), INTERLEAVE IN PARENT T;
GRANT SELECT ON QUEUE Q TO ROLE R;
`,
			expected: []string{
				`DROP QUEUE Q`,
				`DROP TABLE T`,
				"CREATE TABLE T (\n  Id STRING(36) NOT NULL\n) PRIMARY KEY (Id)",
				"CREATE QUEUE Q (\n  Id STRING(36) NOT NULL,\n  Payload BYTES(MAX) NOT NULL\n) PRIMARY KEY (Id),\n  INTERLEAVE IN PARENT T",
				`GRANT SELECT ON QUEUE Q TO ROLE R`,
			},
		},
		{
			name: "drop role with grant on queue",
			from: `
CREATE ROLE R;
CREATE QUEUE Q (Id INT64 NOT NULL, Payload BYTES(MAX) NOT NULL) PRIMARY KEY (Id);
GRANT SELECT ON QUEUE Q TO ROLE R;
`,
			to: `
CREATE QUEUE Q (Id INT64 NOT NULL, Payload BYTES(MAX) NOT NULL) PRIMARY KEY (Id);
`,
			expected: []string{
				`REVOKE SELECT ON QUEUE Q FROM ROLE R`,
				`DROP ROLE R`,
			},
		},
		{
			name: "drop role and queue with grant on queue",
			from: `
CREATE ROLE R;
CREATE QUEUE Q (Id INT64 NOT NULL, Payload BYTES(MAX) NOT NULL) PRIMARY KEY (Id);
GRANT SELECT ON QUEUE Q TO ROLE R;
`,
			to: ``,
			expected: []string{
				`DROP QUEUE Q`,
				`DROP ROLE R`,
			},
		},
	})
}
