package hammer_test

import "testing"

func TestDiffForeignKey(t *testing.T) {
	runDiffTests(t, []diffTestCase{
		{
			name: "create table with constraint",
			from: `
		`,
			to: `
CREATE TABLE t2 (
  t2_1 INT64 NOT NULL,
  CONSTRAINT FK_t2_1 FOREIGN KEY (t2_1) REFERENCES t1 (t1_1),
) PRIMARY KEY(t2_1);
		`,
			expected: []string{
				`
CREATE TABLE t2 (
  t2_1 INT64 NOT NULL,
  CONSTRAINT FK_t2_1 FOREIGN KEY (t2_1) REFERENCES t1 (t1_1)
) PRIMARY KEY (t2_1)`,
			},
		},
		{
			name: "add named constraint",
			from: `
CREATE TABLE t2 (
  t2_1 INT64 NOT NULL,
) PRIMARY KEY(t2_1);
		`,
			to: `
CREATE TABLE t2 (
  t2_1 INT64 NOT NULL,
  CONSTRAINT FK_t2_1 FOREIGN KEY (t2_1) REFERENCES t1 (t1_1),
) PRIMARY KEY(t2_1);
		`,
			expected: []string{
				`ALTER TABLE t2 ADD CONSTRAINT FK_t2_1 FOREIGN KEY (t2_1) REFERENCES t1 (t1_1)`,
			},
		},
		{
			name: "add unnamed constraint",
			from: `
CREATE TABLE t2 (
  t2_1 INT64 NOT NULL,
) PRIMARY KEY(t2_1);
		`,
			to: `
CREATE TABLE t2 (
  t2_1 INT64 NOT NULL,
  FOREIGN KEY (t2_1) REFERENCES t1 (t1_1),
) PRIMARY KEY(t2_1);
		`,
			expected: []string{
				`ALTER TABLE t2 ADD FOREIGN KEY (t2_1) REFERENCES t1 (t1_1)`,
			},
		},
		{
			name: "update named constraint",
			from: `
CREATE TABLE t2 (
  t2_1 INT64 NOT NULL,
  t2_2 INT64 NOT NULL,
  CONSTRAINT FK_t2 FOREIGN KEY (t2_1) REFERENCES t1 (t1_1),
) PRIMARY KEY(t2_1);
		`,
			to: `
CREATE TABLE t2 (
  t2_1 INT64 NOT NULL,
  t2_2 INT64 NOT NULL,
  CONSTRAINT FK_t2 FOREIGN KEY (t2_2) REFERENCES t1 (t1_1),
) PRIMARY KEY(t2_1);
		`,
			expected: []string{
				`ALTER TABLE t2 DROP CONSTRAINT FK_t2`,
				`ALTER TABLE t2 ADD CONSTRAINT FK_t2 FOREIGN KEY (t2_2) REFERENCES t1 (t1_1)`,
			},
		},
		{
			name: "update unnamed constraint",
			from: `
CREATE TABLE t2 (
  t2_1 INT64 NOT NULL,
  t2_2 INT64 NOT NULL,
  FOREIGN KEY (t2_1) REFERENCES t1 (t1_1),
) PRIMARY KEY(t2_1);
		`,
			to: `
CREATE TABLE t2 (
  t2_1 INT64 NOT NULL,
  t2_2 INT64 NOT NULL,
  FOREIGN KEY (t2_1) REFERENCES t1 (t1_1),
  FOREIGN KEY (t2_2) REFERENCES t1 (t1_1),
) PRIMARY KEY(t2_1);
		`,
			expected: []string{
				`ALTER TABLE t2 ADD FOREIGN KEY (t2_2) REFERENCES t1 (t1_1)`,
			},
		},
		{
			name: "drop named constraint",
			from: `
CREATE TABLE t2 (
  t2_1 INT64 NOT NULL,
  CONSTRAINT FK_t2 FOREIGN KEY (t2_1) REFERENCES t1 (t1_1),
) PRIMARY KEY(t2_1);
		`,
			to: `
CREATE TABLE t2 (
  t2_1 INT64 NOT NULL,
) PRIMARY KEY(t2_1);
		`,
			expected: []string{
				`ALTER TABLE t2 DROP CONSTRAINT FK_t2`,
			},
		},
		{
			name: "drop unnamed constraint",
			from: `
CREATE TABLE t2 (
  t2_1 INT64 NOT NULL,
  FOREIGN KEY (t2_1) REFERENCES t1 (t1_1),
) PRIMARY KEY(t2_1);
		`,
			to: `
CREATE TABLE t2 (
  t2_1 INT64 NOT NULL,
) PRIMARY KEY(t2_1);
		`,
			expected: []string{},
		},
		{
			name: "update constraint referencing new column",
			from: `
CREATE TABLE t2 (
  t2_1 INT64,
) PRIMARY KEY(t2_1);
		`,
			to: `
CREATE TABLE t2 (
  t2_1 INT64,
  t2_2 INT64,
  FOREIGN KEY (t2_2) REFERENCES t1 (t1_1),
) PRIMARY KEY(t2_1);
		`,
			expected: []string{
				`ALTER TABLE t2 ADD COLUMN t2_2 INT64`,
				`ALTER TABLE t2 ADD FOREIGN KEY (t2_2) REFERENCES t1 (t1_1)`,
			},
		},
		{
			name: "drop constraint referencing dropped column",
			from: `
CREATE TABLE t2 (
  t2_1 INT64,
  t2_2 INT64,
  CONSTRAINT FK_t2 FOREIGN KEY (t2_2) REFERENCES t1 (t1_1),
) PRIMARY KEY(t2_1);
		`,
			to: `
CREATE TABLE t2 (
  t2_1 INT64,
) PRIMARY KEY(t2_1);
		`,
			expected: []string{
				`ALTER TABLE t2 DROP CONSTRAINT FK_t2`,
				`ALTER TABLE t2 DROP COLUMN t2_2`,
			},
		},
		{
			name: "do not drop unrelated constraint (same column name)",
			from: `
CREATE TABLE t1 (
  t1_1 INT64 NOT NULL,
  col_1 INT64,
) PRIMARY KEY(t1_1);

CREATE TABLE t2 (
  t2_1 INT64 NOT NULL,
) PRIMARY KEY(t2_1);

CREATE TABLE t3 (
  t3_1 INT64 NOT NULL,
  col_1 INT64,
  CONSTRAINT FK_t3 FOREIGN KEY (col_1) REFERENCES t2 (t2_1),
) PRIMARY KEY(t3_1);
		`,
			to: `
CREATE TABLE t1 (
  t1_1 INT64 NOT NULL,
) PRIMARY KEY(t1_1);

CREATE TABLE t2 (
  t2_1 INT64 NOT NULL,
) PRIMARY KEY(t2_1);

CREATE TABLE t3 (
  t3_1 INT64 NOT NULL,
  col_1 INT64,
  CONSTRAINT FK_t3 FOREIGN KEY (col_1) REFERENCES t2 (t2_1),
) PRIMARY KEY(t3_1);
		`,
			expected: []string{
				`ALTER TABLE t1 DROP COLUMN col_1`,
			},
		},
		{
			name: "do not drop unrelated constraint (same referenced column name)",
			from: `
CREATE TABLE t1 (
  t1_1 INT64 NOT NULL,
  col_1 INT64,
) PRIMARY KEY(t1_1);

CREATE TABLE t2 (
  col_1 INT64 NOT NULL,
) PRIMARY KEY(col_1);

CREATE TABLE t3 (
  t3_1 INT64 NOT NULL,
  t3_2 INT64,
  CONSTRAINT FK_t3 FOREIGN KEY (t3_2) REFERENCES t2 (col_1),
) PRIMARY KEY(t3_1);
		`,
			to: `
CREATE TABLE t1 (
  t1_1 INT64 NOT NULL,
) PRIMARY KEY(t1_1);

CREATE TABLE t2 (
  col_1 INT64 NOT NULL,
) PRIMARY KEY(col_1);

CREATE TABLE t3 (
  t3_1 INT64 NOT NULL,
  t3_2 INT64,
  CONSTRAINT FK_t3 FOREIGN KEY (t3_2) REFERENCES t2 (col_1),
) PRIMARY KEY(t3_1);
		`,
			expected: []string{
				`ALTER TABLE t1 DROP COLUMN col_1`,
			},
		},
		{
			name: "drop constraint referencing dropped table",
			from: `
CREATE TABLE t1 (
  t1_1 INT64,
) PRIMARY KEY(t1_1);

CREATE TABLE t2 (
  t2_1 INT64,
  CONSTRAINT FK_t2 FOREIGN KEY (t2_1) REFERENCES t1 (t1_1),
) PRIMARY KEY(t2_1);
		`,
			to: `
CREATE TABLE t2 (
  t2_1 INT64,
) PRIMARY KEY(t2_1);
		`,
			expected: []string{
				`ALTER TABLE t2 DROP CONSTRAINT FK_t2`,
				`DROP TABLE t1`,
			},
		},
		{
			name: "drop multiple named constraints referencing dropped column",
			from: `
CREATE TABLE t1 (
  t1_1 INT64,
  t1_2 INT64,
) PRIMARY KEY(t1_2);

CREATE TABLE t2 (
  t2_1 INT64,
  CONSTRAINT FK_t2 FOREIGN KEY (t2_1) REFERENCES t1 (t1_1),
) PRIMARY KEY(t2_1);

CREATE TABLE t3 (
  t3_1 INT64,
  CONSTRAINT FK_t3 FOREIGN KEY (t3_1) REFERENCES t1 (t1_1),
) PRIMARY KEY(t3_1);
		`,
			to: `
CREATE TABLE t1 (
  t1_2 INT64,
) PRIMARY KEY(t1_2);

CREATE TABLE t2 (
  t2_1 INT64,
) PRIMARY KEY(t2_1);

CREATE TABLE t3 (
  t3_1 INT64,
) PRIMARY KEY(t3_1);
		`,
			expected: []string{
				`ALTER TABLE t2 DROP CONSTRAINT FK_t2`,
				`ALTER TABLE t3 DROP CONSTRAINT FK_t3`,
				`ALTER TABLE t1 DROP COLUMN t1_1`,
			},
		},
		{
			name: "drop named constraint referencing multiple dropped columns",
			from: `
CREATE TABLE t1 (
  t1_1 INT64,
  t1_2 INT64,
  t1_3 INT64,
) PRIMARY KEY(t1_3);

CREATE TABLE t2 (
  t2_1 INT64,
  t2_2 INT64,
  CONSTRAINT FK_t2 FOREIGN KEY (t2_1,t2_2) REFERENCES t1 (t1_1,t1_2),
) PRIMARY KEY(t2_1);
		`,
			to: `
CREATE TABLE t1 (
  t1_3 INT64,
) PRIMARY KEY(t1_3);

CREATE TABLE t2 (
  t2_1 INT64,
  t2_2 INT64,
) PRIMARY KEY(t2_1);
		`,
			expected: []string{
				`ALTER TABLE t2 DROP CONSTRAINT FK_t2`,
				`ALTER TABLE t1 DROP COLUMN t1_1`,
				`ALTER TABLE t1 DROP COLUMN t1_2`,
			},
		},
		{
			name: "update constraint referencing dropped column",
			from: `
CREATE TABLE t1 (
  t1_1 INT64,
  t1_2 INT64,
) PRIMARY KEY(t1_2);

CREATE TABLE t2 (
  t2_1 INT64,
  CONSTRAINT FK_t2 FOREIGN KEY (t2_1) REFERENCES t1 (t1_1),
) PRIMARY KEY(t2_1);
		`,
			to: `
CREATE TABLE t1 (
  t1_2 INT64,
) PRIMARY KEY(t1_2);

CREATE TABLE t2 (
  t2_1 INT64,
  CONSTRAINT FK_t2 FOREIGN KEY (t2_1) REFERENCES t1 (t1_2),
) PRIMARY KEY(t2_1);
		`,
			expected: []string{
				`ALTER TABLE t2 DROP CONSTRAINT FK_t2`,
				`ALTER TABLE t1 DROP COLUMN t1_1`,
				`ALTER TABLE t2 ADD CONSTRAINT FK_t2 FOREIGN KEY (t2_1) REFERENCES t1 (t1_2)`,
			},
		},
		{
			name: "recreate constraint if recreating referenced table",
			from: `
CREATE TABLE t1 (
  t1_1 INT64,
  t1_2 INT64,
) PRIMARY KEY(t1_1);

CREATE TABLE t2 (
  t2_1 INT64,
  CONSTRAINT FK_t2 FOREIGN KEY (t2_1) REFERENCES t1 (t1_1),
) PRIMARY KEY(t2_1);
		`,
			to: `
CREATE TABLE t1 (
  t1_1 INT64,
  t1_2 INT64,
) PRIMARY KEY(t1_2);

CREATE TABLE t2 (
  t2_1 INT64,
  CONSTRAINT FK_t2 FOREIGN KEY (t2_1) REFERENCES t1 (t1_1),
) PRIMARY KEY(t2_1);
		`,
			expected: []string{
				`ALTER TABLE t2 DROP CONSTRAINT FK_t2`,
				`DROP TABLE t1`,
				`
CREATE TABLE t1 (
  t1_1 INT64,
  t1_2 INT64
) PRIMARY KEY (t1_2)`,
				`ALTER TABLE t2 ADD CONSTRAINT FK_t2 FOREIGN KEY (t2_1) REFERENCES t1 (t1_1)`,
			},
		},
		{
			name: "do not recreate constraint for default delete action",
			from: `
CREATE TABLE t1 (
  t1_1 INT64,
) PRIMARY KEY(t1_1);

CREATE TABLE t2 (
  t2_1 INT64,
  CONSTRAINT FK_t2 FOREIGN KEY (t2_1) REFERENCES t1 (t1_1) ON DELETE NO ACTION,
) PRIMARY KEY(t2_1);
		`,
			to: `
CREATE TABLE t1 (
  t1_1 INT64,
) PRIMARY KEY(t1_1);

CREATE TABLE t2 (
  t2_1 INT64,
  CONSTRAINT FK_t2 FOREIGN KEY (t2_1) REFERENCES t1 (t1_1),
) PRIMARY KEY(t2_1);
		`,
			expected: []string{},
		},
		{
			name: "add foreign key with ALTER TABLE",
			from: `
CREATE TABLE t2 (
  t2_1 INT64,
) PRIMARY KEY(t2_1);
		`,
			to: `
CREATE TABLE t2 (
  t2_1 INT64,
) PRIMARY KEY(t2_1);
ALTER TABLE t2 ADD CONSTRAINT FK_t2 FOREIGN KEY (t2_1) REFERENCES t1 (t1_1);
		`,
			expected: []string{
				`ALTER TABLE t2 ADD CONSTRAINT FK_t2 FOREIGN KEY (t2_1) REFERENCES t1 (t1_1)`,
			},
		},
		{
			name: "whitespace-only difference is no-op",
			from: `


CREATE TABLE t2 (
  t2_1 INT64 NOT NULL,
  t2_2 INT64 NOT NULL,
  CONSTRAINT FK_t2 FOREIGN KEY (t2_1) REFERENCES t1 (t1_1),
) PRIMARY KEY(t2_1);
CREATE INDEX idx_t2_1 ON t2(t2_1);
		`,
			to: `
CREATE TABLE t2 (
  t2_1 INT64 NOT NULL,
  t2_2 INT64 NOT NULL,
  CONSTRAINT FK_t2 FOREIGN KEY (t2_1) REFERENCES t1 (t1_1),
) PRIMARY KEY(t2_1);
CREATE INDEX idx_t2_1 ON t2(t2_1);
		`,
			expected: []string{},
		},
		{
			name: "foreign key enforced is the default (explicit to implicit)",
			from: `
CREATE TABLE t1 (
  t1_1 INT64 NOT NULL,
) PRIMARY KEY(t1_1);
CREATE TABLE t2 (
  t2_1 INT64 NOT NULL,
  t2_2 INT64,
  CONSTRAINT FK_t2 FOREIGN KEY (t2_2) REFERENCES t1 (t1_1) ENFORCED,
) PRIMARY KEY(t2_1);
`,
			to: `
CREATE TABLE t1 (
  t1_1 INT64 NOT NULL,
) PRIMARY KEY(t1_1);
CREATE TABLE t2 (
  t2_1 INT64 NOT NULL,
  t2_2 INT64,
  CONSTRAINT FK_t2 FOREIGN KEY (t2_2) REFERENCES t1 (t1_1),
) PRIMARY KEY(t2_1);
`,
			expected: []string{},
		},
		{
			name: "foreign key enforced is the default (implicit to explicit)",
			from: `
CREATE TABLE t1 (
  t1_1 INT64 NOT NULL,
) PRIMARY KEY(t1_1);
CREATE TABLE t2 (
  t2_1 INT64 NOT NULL,
  t2_2 INT64,
  CONSTRAINT FK_t2 FOREIGN KEY (t2_2) REFERENCES t1 (t1_1),
) PRIMARY KEY(t2_1);
`,
			to: `
CREATE TABLE t1 (
  t1_1 INT64 NOT NULL,
) PRIMARY KEY(t1_1);
CREATE TABLE t2 (
  t2_1 INT64 NOT NULL,
  t2_2 INT64,
  CONSTRAINT FK_t2 FOREIGN KEY (t2_2) REFERENCES t1 (t1_1) ENFORCED,
) PRIMARY KEY(t2_1);
`,
			expected: []string{},
		},
		{
			name: "foreign key not enforced is detected as a real change",
			from: `
CREATE TABLE t1 (
  t1_1 INT64 NOT NULL,
) PRIMARY KEY(t1_1);
CREATE TABLE t2 (
  t2_1 INT64 NOT NULL,
  t2_2 INT64,
  CONSTRAINT FK_t2 FOREIGN KEY (t2_2) REFERENCES t1 (t1_1),
) PRIMARY KEY(t2_1);
`,
			to: `
CREATE TABLE t1 (
  t1_1 INT64 NOT NULL,
) PRIMARY KEY(t1_1);
CREATE TABLE t2 (
  t2_1 INT64 NOT NULL,
  t2_2 INT64,
  CONSTRAINT FK_t2 FOREIGN KEY (t2_2) REFERENCES t1 (t1_1) NOT ENFORCED,
) PRIMARY KEY(t2_1);
`,
			expected: []string{
				"ALTER TABLE t2 DROP CONSTRAINT FK_t2",
				"ALTER TABLE t2 ADD CONSTRAINT FK_t2 FOREIGN KEY (t2_2) REFERENCES t1 (t1_1) NOT ENFORCED",
			},
		},
		{
			name: "foreign key inline is equivalent to ALTER TABLE ADD CONSTRAINT",
			from: `
CREATE TABLE parent (
  pid INT64 NOT NULL,
) PRIMARY KEY(pid);
CREATE TABLE child (
  cid INT64 NOT NULL,
  ref INT64,
  CONSTRAINT FK_child FOREIGN KEY (ref) REFERENCES parent (pid),
) PRIMARY KEY(cid);
`,
			to: `
CREATE TABLE parent (
  pid INT64 NOT NULL,
) PRIMARY KEY(pid);
CREATE TABLE child (
  cid INT64 NOT NULL,
  ref INT64,
) PRIMARY KEY(cid);
ALTER TABLE child ADD CONSTRAINT FK_child FOREIGN KEY (ref) REFERENCES parent (pid);
`,
			expected: []string{},
		},
	})
}

func TestDiffCheckConstraint(t *testing.T) {
	runDiffTests(t, []diffTestCase{
		{
			name: "create table with check constraint",
			from: ``,
			to: `
CREATE TABLE t1 (
  t1_1 INT64 NOT NULL,
  t1_2 INT64,
  CONSTRAINT CK_t1 CHECK (t1_2 > 0),
) PRIMARY KEY(t1_1);
`,
			expected: []string{
				`
CREATE TABLE t1 (
  t1_1 INT64 NOT NULL,
  t1_2 INT64,
  CONSTRAINT CK_t1 CHECK (t1_2 > 0)
) PRIMARY KEY (t1_1)`,
			},
		},
		{
			name: "add named check constraint",
			from: `
CREATE TABLE t1 (
  t1_1 INT64 NOT NULL,
  t1_2 INT64,
) PRIMARY KEY(t1_1);
`,
			to: `
CREATE TABLE t1 (
  t1_1 INT64 NOT NULL,
  t1_2 INT64,
  CONSTRAINT CK_t1 CHECK (t1_2 > 0),
) PRIMARY KEY(t1_1);
`,
			expected: []string{
				`ALTER TABLE t1 ADD CONSTRAINT CK_t1 CHECK (t1_2 > 0)`,
			},
		},
		{
			name: "add unnamed check constraint",
			from: `
CREATE TABLE t1 (
  t1_1 INT64 NOT NULL,
  t1_2 INT64,
) PRIMARY KEY(t1_1);
`,
			to: `
CREATE TABLE t1 (
  t1_1 INT64 NOT NULL,
  t1_2 INT64,
  CHECK (t1_2 > 0),
) PRIMARY KEY(t1_1);
`,
			expected: []string{
				`ALTER TABLE t1 ADD CHECK (t1_2 > 0)`,
			},
		},
		{
			name: "update named check constraint",
			from: `
CREATE TABLE t1 (
  t1_1 INT64 NOT NULL,
  t1_2 INT64,
  CONSTRAINT CK_t1 CHECK (t1_2 > 0),
) PRIMARY KEY(t1_1);
`,
			to: `
CREATE TABLE t1 (
  t1_1 INT64 NOT NULL,
  t1_2 INT64,
  CONSTRAINT CK_t1 CHECK (t1_2 > 1),
) PRIMARY KEY(t1_1);
`,
			expected: []string{
				`ALTER TABLE t1 DROP CONSTRAINT CK_t1`,
				`ALTER TABLE t1 ADD CONSTRAINT CK_t1 CHECK (t1_2 > 1)`,
			},
		},
		{
			name: "drop named check constraint",
			from: `
CREATE TABLE t1 (
  t1_1 INT64 NOT NULL,
  t1_2 INT64,
  CONSTRAINT CK_t1 CHECK (t1_2 > 0),
) PRIMARY KEY(t1_1);
`,
			to: `
CREATE TABLE t1 (
  t1_1 INT64 NOT NULL,
  t1_2 INT64,
) PRIMARY KEY(t1_1);
`,
			expected: []string{
				`ALTER TABLE t1 DROP CONSTRAINT CK_t1`,
			},
		},
		{
			name: "drop unnamed check constraint",
			from: `
CREATE TABLE t1 (
  t1_1 INT64 NOT NULL,
  t1_2 INT64,
  CHECK (t1_2 > 0),
) PRIMARY KEY(t1_1);
`,
			to: `
CREATE TABLE t1 (
  t1_1 INT64 NOT NULL,
  t1_2 INT64,
) PRIMARY KEY(t1_1);
`,
			expected: []string{},
		},
		{
			name: "drop check constraint before the column it references",
			from: `
CREATE TABLE t1 (
  t1_1 INT64 NOT NULL,
  t1_2 INT64,
  CONSTRAINT CK_t1 CHECK (t1_2 > 0),
) PRIMARY KEY(t1_1);
`,
			to: `
CREATE TABLE t1 (
  t1_1 INT64 NOT NULL,
) PRIMARY KEY(t1_1);
`,
			expected: []string{
				`ALTER TABLE t1 DROP CONSTRAINT CK_t1`,
				`ALTER TABLE t1 DROP COLUMN t1_2`,
			},
		},
		{
			name: "recreate check constraint when recreating the column it references",
			from: `
CREATE TABLE t1 (
  t1_1 INT64 NOT NULL,
  t1_2 INT64,
  CONSTRAINT CK_t1 CHECK (t1_2 IS NOT NULL),
) PRIMARY KEY(t1_1);
`,
			to: `
CREATE TABLE t1 (
  t1_1 INT64 NOT NULL,
  t1_2 STRING(MAX),
  CONSTRAINT CK_t1 CHECK (t1_2 IS NOT NULL),
) PRIMARY KEY(t1_1);
`,
			expected: []string{
				`ALTER TABLE t1 DROP CONSTRAINT CK_t1`,
				`ALTER TABLE t1 DROP COLUMN t1_2`,
				`ALTER TABLE t1 ADD COLUMN t1_2 STRING(MAX)`,
				`ALTER TABLE t1 ADD CONSTRAINT CK_t1 CHECK (t1_2 IS NOT NULL)`,
			},
		},
		{
			name: "identifier matching the dropped column name recreates the check constraint",
			from: `
CREATE TABLE t1 (
  t1_1 INT64 NOT NULL,
  length INT64,
  name STRING(MAX),
  CONSTRAINT CK_t1 CHECK (length(name) > 0),
) PRIMARY KEY(t1_1);
`,
			to: `
CREATE TABLE t1 (
  t1_1 INT64 NOT NULL,
  name STRING(MAX),
  CONSTRAINT CK_t1 CHECK (length(name) > 0),
) PRIMARY KEY(t1_1);
`,
			expected: []string{
				`ALTER TABLE t1 DROP CONSTRAINT CK_t1`,
				`ALTER TABLE t1 DROP COLUMN length`,
				`ALTER TABLE t1 ADD CONSTRAINT CK_t1 CHECK (length(name) > 0)`,
			},
		},
		{
			name: "re-add check constraint when the table name differs only in case",
			from: `
CREATE TABLE t1 (
  t1_1 INT64 NOT NULL,
  t1_2 INT64,
  CONSTRAINT CK_t1 CHECK (t1_2 IS NOT NULL),
) PRIMARY KEY(t1_1);
`,
			to: `
CREATE TABLE T1 (
  t1_1 INT64 NOT NULL,
  t1_2 STRING(MAX),
  CONSTRAINT CK_t1 CHECK (t1_2 IS NOT NULL),
) PRIMARY KEY(t1_1);
`,
			expected: []string{
				`ALTER TABLE t1 DROP CONSTRAINT CK_t1`,
				`ALTER TABLE t1 DROP COLUMN t1_2`,
				`ALTER TABLE T1 ADD COLUMN t1_2 STRING(MAX)`,
				`ALTER TABLE T1 ADD CONSTRAINT CK_t1 CHECK (t1_2 IS NOT NULL)`,
			},
		},
		{
			name: "JSON member names in check constraint are case-sensitive",
			from: `
CREATE TABLE t1 (
  t1_1 INT64 NOT NULL,
  payload JSON,
  CONSTRAINT CK_t1 CHECK (payload.Status IS NOT NULL),
) PRIMARY KEY(t1_1);
`,
			to: `
CREATE TABLE t1 (
  t1_1 INT64 NOT NULL,
  payload JSON,
  CONSTRAINT CK_t1 CHECK (payload.status IS NOT NULL),
) PRIMARY KEY(t1_1);
`,
			expected: []string{
				`ALTER TABLE t1 DROP CONSTRAINT CK_t1`,
				`ALTER TABLE t1 ADD CONSTRAINT CK_t1 CHECK (payload.status IS NOT NULL)`,
			},
		},
		{
			name: "recreating a column does not re-add a same-named check constraint on another table",
			from: `
CREATE SCHEMA s1;
CREATE SCHEMA s2;
CREATE TABLE s1.t1 (
  t1_1 INT64 NOT NULL,
  t1_2 INT64,
  CONSTRAINT CK_t1 CHECK (t1_2 IS NOT NULL),
) PRIMARY KEY(t1_1);
CREATE TABLE s2.t1 (
  t1_1 INT64 NOT NULL,
  t1_2 INT64,
  CONSTRAINT CK_t1 CHECK (t1_2 IS NOT NULL),
) PRIMARY KEY(t1_1);
`,
			to: `
CREATE SCHEMA s1;
CREATE SCHEMA s2;
CREATE TABLE s1.t1 (
  t1_1 INT64 NOT NULL,
  t1_2 STRING(MAX),
  CONSTRAINT CK_t1 CHECK (t1_2 IS NOT NULL),
) PRIMARY KEY(t1_1);
CREATE TABLE s2.t1 (
  t1_1 INT64 NOT NULL,
  t1_2 INT64,
  CONSTRAINT CK_t1 CHECK (t1_2 IS NOT NULL),
) PRIMARY KEY(t1_1);
`,
			expected: []string{
				`ALTER TABLE s1.t1 DROP CONSTRAINT CK_t1`,
				`ALTER TABLE s1.t1 DROP COLUMN t1_2`,
				`ALTER TABLE s1.t1 ADD COLUMN t1_2 STRING(MAX)`,
				`ALTER TABLE s1.t1 ADD CONSTRAINT CK_t1 CHECK (t1_2 IS NOT NULL)`,
			},
		},
		{
			name: "do not drop check constraint referencing only other columns",
			from: `
CREATE TABLE t1 (
  t1_1 INT64 NOT NULL,
  t1_2 INT64,
  t1_3 INT64,
  CONSTRAINT CK_t1 CHECK (t1_3 > 0),
) PRIMARY KEY(t1_1);
`,
			to: `
CREATE TABLE t1 (
  t1_1 INT64 NOT NULL,
  t1_3 INT64,
  CONSTRAINT CK_t1 CHECK (t1_3 > 0),
) PRIMARY KEY(t1_1);
`,
			expected: []string{
				`ALTER TABLE t1 DROP COLUMN t1_2`,
			},
		},
	})
}
