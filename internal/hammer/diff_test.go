package hammer_test

import (
	"context"
	"reflect"
	"strings"
	"testing"

	"github.com/daichirata/hammer/internal/hammer"
)

type StringSource string

func (s StringSource) String() string { return string(s) }
func (s StringSource) DDL(context.Context) (hammer.DDL, error) {
	return hammer.ParseDDL("string", s.String())
}

func TestDiff(t *testing.T) {
	values := []struct {
		name     string
		from     string
		to       string
		expected []string
	}{
		{
			name: "drop table",
			from: `
CREATE TABLE t1 (
  t1_1 INT64 NOT NULL,
) PRIMARY KEY(t1_1);

CREATE TABLE t2 (
  t2_1 INT64 NOT NULL,
) PRIMARY KEY(t2_1);
`,
			to: `
CREATE TABLE t1 (
  t1_1 INT64 NOT NULL,
) PRIMARY KEY(t1_1);
`,
			expected: []string{
				`DROP TABLE t2`,
			},
		},
		{
			name: "create table",
			from: `
CREATE TABLE t1 (
  t1_1 INT64 NOT NULL,
) PRIMARY KEY(t1_1);
`,
			to: `
CREATE TABLE t1 (
  t1_1 INT64 NOT NULL,
) PRIMARY KEY(t1_1);

CREATE TABLE t2 (
  t2_1 INT64 NOT NULL,
) PRIMARY KEY(t2_1);
`,
			expected: []string{
				`CREATE TABLE t2 (
  t2_1 INT64 NOT NULL,
) PRIMARY KEY(t2_1)`,
			},
		},
		{
			name: "drop column (different column positions)",
			from: `
CREATE TABLE t1 (
  t1_2 INT64 NOT NULL,
  t1_1 INT64 NOT NULL,
) PRIMARY KEY(t1_1);
`,
			to: `
CREATE TABLE t1 (
  t1_1 INT64 NOT NULL,
) PRIMARY KEY(t1_1);
`,
			expected: []string{
				`ALTER TABLE t1 DROP COLUMN t1_2`,
			},
		},
		{
			name: "add column (allow null)",
			from: `
CREATE TABLE t1 (
  t1_1 INT64 NOT NULL,
) PRIMARY KEY(t1_1);
`,
			to: `
CREATE TABLE t1 (
  t1_1 INT64 NOT NULL,
  t1_2 INT64,
) PRIMARY KEY(t1_1);
`,
			expected: []string{
				`ALTER TABLE t1 ADD COLUMN t1_2 INT64`,
			},
		},
		{
			name: "add column (not null)",
			from: `
CREATE TABLE t1 (
  t1_1 INT64 NOT NULL,
) PRIMARY KEY(t1_1);
`,
			to: `
CREATE TABLE t1 (
  t1_1 INT64 NOT NULL,
  t1_2 INT64 NOT NULL,
) PRIMARY KEY(t1_1);
`,
			expected: []string{
				`ALTER TABLE t1 ADD COLUMN t1_2 INT64`,
				`UPDATE t1 SET t1_2 = 0 WHERE t1_2 IS NULL`,
				`ALTER TABLE t1 ALTER COLUMN t1_2 INT64 NOT NULL`,
			},
		},
		{
			name: "change column (different type)",
			from: `
CREATE TABLE t1 (
  t1_1 INT64 NOT NULL,
  t1_2 INT64,
) PRIMARY KEY(t1_1);
`,
			to: `
CREATE TABLE t1 (
  t1_1 INT64 NOT NULL,
  t1_2 STRING(36),
) PRIMARY KEY(t1_1);
`,
			expected: []string{
				`ALTER TABLE t1 DROP COLUMN t1_2`,
				`ALTER TABLE t1 ADD COLUMN t1_2 STRING(36)`,
			},
		},
		{
			name: "change column (same type)",
			from: `
CREATE TABLE t1 (
  t1_1 INT64 NOT NULL,
  t1_2 STRING(36) NOT NULL,
) PRIMARY KEY(t1_1);
`,
			to: `
CREATE TABLE t1 (
  t1_1 INT64 NOT NULL,
  t1_2 STRING(50) NOT NULL,
) PRIMARY KEY(t1_1);
`,
			expected: []string{
				`ALTER TABLE t1 ALTER COLUMN t1_2 STRING(50) NOT NULL`,
			},
		},
		{
			name: "change column (timestamp)",
			from: `
CREATE TABLE t1 (
  t1_1 INT64 NOT NULL,
  t1_2 TIMESTAMP,
) PRIMARY KEY(t1_1);
`,
			to: `
CREATE TABLE t1 (
  t1_1 INT64 NOT NULL,
  t1_2 TIMESTAMP NOT NULL,
) PRIMARY KEY(t1_1);
`,
			expected: []string{
				`UPDATE t1 SET t1_2 = '0001-01-01T00:00:00Z' WHERE t1_2 IS NULL`,
				`ALTER TABLE t1 ALTER COLUMN t1_2 TIMESTAMP NOT NULL`,
			},
		},
		{
			name: "change column (timestamp)",
			from: `
CREATE TABLE t1 (
  t1_1 INT64 NOT NULL,
  t1_2 TIMESTAMP NOT NULL,
) PRIMARY KEY(t1_1);
`,
			to: `
CREATE TABLE t1 (
  t1_1 INT64 NOT NULL,
  t1_2 TIMESTAMP,
) PRIMARY KEY(t1_1);
`,
			expected: []string{
				`ALTER TABLE t1 ALTER COLUMN t1_2 TIMESTAMP`,
			},
		},
		{
			name: "change column (timestamp)",
			from: `
CREATE TABLE t1 (
  t1_1 INT64 NOT NULL,
  t1_2 TIMESTAMP NOT NULL,
) PRIMARY KEY(t1_1);
`,
			to: `
CREATE TABLE t1 (
  t1_1 INT64 NOT NULL,
  t1_2 TIMESTAMP NOT NULL OPTIONS (allow_commit_timestamp = true),
) PRIMARY KEY(t1_1);
`,
			expected: []string{
				`ALTER TABLE t1 ALTER COLUMN t1_2 SET OPTIONS (allow_commit_timestamp = true)`,
			},
		},
		{
			name: "change column (timestamp)",
			from: `
CREATE TABLE t1 (
  t1_1 INT64 NOT NULL,
  t1_2 TIMESTAMP,
) PRIMARY KEY(t1_1);
`,
			to: `
CREATE TABLE t1 (
  t1_1 INT64 NOT NULL,
  t1_2 TIMESTAMP NOT NULL OPTIONS (allow_commit_timestamp = true),
) PRIMARY KEY(t1_1);
`,
			expected: []string{
				`UPDATE t1 SET t1_2 = '0001-01-01T00:00:00Z' WHERE t1_2 IS NULL`,
				`ALTER TABLE t1 ALTER COLUMN t1_2 TIMESTAMP NOT NULL`,
				`ALTER TABLE t1 ALTER COLUMN t1_2 SET OPTIONS (allow_commit_timestamp = true)`,
			},
		},
		{
			name: "change column (timestamp)",
			from: `
CREATE TABLE t1 (
  t1_1 INT64 NOT NULL,
  t1_2 TIMESTAMP NOT NULL OPTIONS (allow_commit_timestamp = true),
) PRIMARY KEY(t1_1);
`,
			to: `
CREATE TABLE t1 (
  t1_1 INT64 NOT NULL,
  t1_2 TIMESTAMP,
) PRIMARY KEY(t1_1);
`,
			expected: []string{
				`ALTER TABLE t1 ALTER COLUMN t1_2 TIMESTAMP`,
				`ALTER TABLE t1 ALTER COLUMN t1_2 SET OPTIONS (allow_commit_timestamp = null)`,
			},
		},
		{
			name: "add generated column",
			from: `
CREATE TABLE t1 (
  t1_1 STRING(36) NOT NULL,
) PRIMARY KEY(t1_1);
`,
			to: `
CREATE TABLE t1 (
  t1_1 STRING(36) NOT NULL,
  t1_2 STRING(1) NOT NULL AS (SUBSTR(t1_1, 1, 1)) STORED,
) PRIMARY KEY(t1_1);
`,
			expected: []string{
				`ALTER TABLE t1 ADD COLUMN t1_2 STRING(1) NOT NULL AS (SUBSTR(t1_1, 1, 1)) STORED`,
			},
		},
		{
			name: "change column to generated column",
			from: `
CREATE TABLE t1 (
  t1_1 STRING(36) NOT NULL,
  t1_2 STRING(1) NOT NULL,
) PRIMARY KEY(t1_1);
`,
			to: `
CREATE TABLE t1 (
  t1_1 STRING(36) NOT NULL,
  t1_2 STRING(1) NOT NULL AS (SUBSTR(t1_1, 1, 1)) STORED,
) PRIMARY KEY(t1_1);
`,
			expected: []string{
				`ALTER TABLE t1 DROP COLUMN t1_2`,
				`ALTER TABLE t1 ADD COLUMN t1_2 STRING(1) NOT NULL AS (SUBSTR(t1_1, 1, 1)) STORED`,
			},
		},
		{
			name: "change column from generated column to normal",
			from: `
CREATE TABLE t1 (
  t1_1 STRING(36) NOT NULL,
  t1_2 STRING(1) NOT NULL AS (SUBSTR(t1_1, 1, 1)) STORED,
) PRIMARY KEY(t1_1);
`,
			to: `
CREATE TABLE t1 (
  t1_1 STRING(36) NOT NULL,
  t1_2 STRING(1) NOT NULL,
) PRIMARY KEY(t1_1);
`,
			expected: []string{
				`ALTER TABLE t1 DROP COLUMN t1_2`,
				`ALTER TABLE t1 ADD COLUMN t1_2 STRING(1)`,
				`UPDATE t1 SET t1_2 = '' WHERE t1_2 IS NULL`,
				`ALTER TABLE t1 ALTER COLUMN t1_2 STRING(1) NOT NULL`,
			},
		},
		{
			name: "add index",
			from: `
CREATE TABLE t1 (
  t1_1 INT64 NOT NULL,
  t1_2 STRING(36) NOT NULL,
  t1_3 STRING(36) NOT NULL,
) PRIMARY KEY(t1_1);
CREATE INDEX idx_t1_1 ON t1(t1_2);
`,
			to: `
CREATE TABLE t1 (
  t1_1 INT64 NOT NULL,
  t1_2 STRING(36) NOT NULL,
  t1_3 STRING(36) NOT NULL,
) PRIMARY KEY(t1_1);
CREATE INDEX idx_t1_1 ON t1(t1_2);
CREATE INDEX idx_t1_2 ON t1(t1_3);
`,
			expected: []string{
				`CREATE INDEX idx_t1_2 ON t1(t1_3)`,
			},
		},
		{
			name: "drop index (different index positions)",
			from: `
CREATE TABLE t1 (
  t1_1 INT64 NOT NULL,
  t1_2 STRING(36) NOT NULL,
  t1_3 STRING(36) NOT NULL,
) PRIMARY KEY(t1_1);

CREATE INDEX idx_t1_1 ON t1(t1_2);

CREATE INDEX idx_t1_2 ON t1(t1_3);

`,
			to: `
CREATE TABLE t1 (
  t1_1 INT64 NOT NULL,
  t1_2 STRING(36) NOT NULL,
  t1_3 STRING(36) NOT NULL,
) PRIMARY KEY(t1_1);
CREATE INDEX idx_t1_2 ON t1(t1_3);
`,
			expected: []string{
				`DROP INDEX idx_t1_1`,
			},
		},
		{
			name: "change indexed column",
			from: `
CREATE TABLE t1 (
  t1_1 INT64 NOT NULL,
  t1_2 STRING(36) NOT NULL,
  t1_3 STRING(36),
) PRIMARY KEY(t1_1);
CREATE INDEX idx_t1_1 ON t1(t1_2);
CREATE INDEX idx_t1_2 ON t1(t1_3);
`,
			to: `
CREATE TABLE t1 (
  t1_1 INT64 NOT NULL,
  t1_2 STRING(36) NOT NULL,
  t1_3 INT64 NOT NULL,
) PRIMARY KEY(t1_1);
CREATE INDEX idx_t1_1 ON t1(t1_2);
CREATE INDEX idx_t1_2 ON t1(t1_3);
`,
			expected: []string{
				`DROP INDEX idx_t1_2`,
				`ALTER TABLE t1 DROP COLUMN t1_3`,
				`ALTER TABLE t1 ADD COLUMN t1_3 INT64`,
				`UPDATE t1 SET t1_3 = 0 WHERE t1_3 IS NULL`,
				`ALTER TABLE t1 ALTER COLUMN t1_3 INT64 NOT NULL`,
				`CREATE INDEX idx_t1_2 ON t1(t1_3)`,
			},
		},
		{
			name: "change column (interleaved)",
			from: `
CREATE TABLE t1 (
  t1_1 INT64 NOT NULL,
) PRIMARY KEY(t1_1);

CREATE TABLE t2 (
  t1_1 INT64 NOT NULL,
  t2_1 INT64 NOT NULL,
) PRIMARY KEY(t1_1, t2_1),
  INTERLEAVE IN PARENT t1 ON DELETE NO ACTION;
CREATE INDEX idx_t2 ON t2(t2_1);

CREATE TABLE t3 (
  t1_1 INT64 NOT NULL,
  t3_1 INT64 NOT NULL,
) PRIMARY KEY(t1_1, t3_1),
  INTERLEAVE IN PARENT t1 ON DELETE NO ACTION;
CREATE INDEX idx_t3 ON t3(t3_1);
`,
			to: `
CREATE TABLE t1 (
  t1_1 STRING(36) NOT NULL,
) PRIMARY KEY(t1_1);

CREATE TABLE t2 (
  t1_1 INT64 NOT NULL,
  t2_1 INT64 NOT NULL,
  t2_2 INT64 NOT NULL,
  t2_3 INT64 NOT NULL,
) PRIMARY KEY(t1_1, t2_1),
  INTERLEAVE IN PARENT t1 ON DELETE NO ACTION;
CREATE INDEX idx_t2 ON t2(t2_1);

CREATE TABLE t3 (
  t1_1 INT64 NOT NULL,
  t3_1 INT64 NOT NULL,
) PRIMARY KEY(t1_1, t3_1),
  INTERLEAVE IN PARENT t1 ON DELETE NO ACTION;
CREATE INDEX idx_t3 ON t3(t3_1);
`,
			expected: []string{
				`DROP INDEX idx_t2`,
				`DROP TABLE t2`,
				`DROP INDEX idx_t3`,
				`DROP TABLE t3`,
				`DROP TABLE t1`,
				`CREATE TABLE t1 (
  t1_1 STRING(36) NOT NULL,
) PRIMARY KEY(t1_1)`,
				`CREATE TABLE t2 (
  t1_1 INT64 NOT NULL,
  t2_1 INT64 NOT NULL,
  t2_2 INT64 NOT NULL,
  t2_3 INT64 NOT NULL,
) PRIMARY KEY(t1_1, t2_1),
  INTERLEAVE IN PARENT t1 ON DELETE NO ACTION`,
				`CREATE INDEX idx_t2 ON t2(t2_1)`,
				`CREATE TABLE t3 (
  t1_1 INT64 NOT NULL,
  t3_1 INT64 NOT NULL,
) PRIMARY KEY(t1_1, t3_1),
  INTERLEAVE IN PARENT t1 ON DELETE NO ACTION`,
				`CREATE INDEX idx_t3 ON t3(t3_1)`,
			},
		},
		{
			name: "Create table with constraint",
			from: `
		`,
			to: `
CREATE TABLE t2 (
  t2_1 INT64 NOT NULL,
  CONSTRAINT FK_t2_1 FOREIGN KEY (t2_1) REFERENCES t1 (t1_1),
) PRIMARY KEY(t2_1);
		`,
			expected: []string{
				`CREATE TABLE t2 (
  t2_1 INT64 NOT NULL,
  CONSTRAINT FK_t2_1 FOREIGN KEY (t2_1) REFERENCES t1 (t1_1),
) PRIMARY KEY(t2_1)`,
			},
		},
		{
			name: "Add named constraint",
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
			name: "Add unnamed constraint",
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
			name: "Update named constraint",
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
			name: "Update unnamed constraint",
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
			name: "Drop named constraint",
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
			name: "Drop unnamed constraint",
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
			name: "Update constraint referencing new column",
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
			name: "Drop constraint referencing dropped column.",
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
			name: "Drop constraint referencing dropped table.",
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
			name: "Drop multiple named constraint referencing dropped column.",
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
			name: "Drop named constraint referencing multiple dropped columns.",
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
			name: "Update constraint referencing dropped column",
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
			name: "Recreate constraint if recreating referenced table.",
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
				`CREATE TABLE t1 (
  t1_1 INT64,
  t1_2 INT64,
) PRIMARY KEY(t1_2)`,
				`ALTER TABLE t2 ADD CONSTRAINT FK_t2 FOREIGN KEY (t2_1) REFERENCES t1 (t1_1)`,
			},
		},
		{
			name: "AlterTable add foreign key",
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
			name: "Only position's diff",
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
			name: "Create table with ROW DELETION POLICY",
			from: ``,
			to: `
CREATE TABLE t1 (
  t1_1 INT64 NOT NULL,
  t1_2 TIMESTAMP NOT NULL,
) PRIMARY KEY(t1_1), ROW DELETION POLICY (OLDER_THAN(t1_2, INTERVAL 30 DAY));
		`,
			expected: []string{
				`CREATE TABLE t1 (
  t1_1 INT64 NOT NULL,
  t1_2 TIMESTAMP NOT NULL,
) PRIMARY KEY(t1_1),
  ROW DELETION POLICY ( OLDER_THAN ( t1_2, INTERVAL 30 DAY ))`,
			},
		},
		{
			name: "Add ROW DELETION POLICY",
			from: `
CREATE TABLE t1 (
  t1_1 INT64 NOT NULL,
  t1_2 TIMESTAMP NOT NULL,
) PRIMARY KEY(t1_1);
		`,
			to: `
CREATE TABLE t1 (
  t1_1 INT64 NOT NULL,
  t1_2 TIMESTAMP NOT NULL,
) PRIMARY KEY(t1_1), ROW DELETION POLICY (OLDER_THAN(t1_2, INTERVAL 30 DAY));
		`,
			expected: []string{
				`ALTER TABLE t1 ADD ROW DELETION POLICY ( OLDER_THAN ( t1_2, INTERVAL 30 DAY ))`,
			},
		},
		{
			name: "Replace ROW DELETION POLICY",
			from: `
CREATE TABLE t1 (
  t1_1 INT64 NOT NULL,
  t1_2 TIMESTAMP NOT NULL,
  t1_3 TIMESTAMP NOT NULL,
) PRIMARY KEY(t1_1), ROW DELETION POLICY (OLDER_THAN(t1_2, INTERVAL 30 DAY));
		`,
			to: `
CREATE TABLE t1 (
  t1_1 INT64 NOT NULL,
  t1_2 TIMESTAMP NOT NULL,
  t1_3 TIMESTAMP NOT NULL,
) PRIMARY KEY(t1_1), ROW DELETION POLICY (OLDER_THAN(t1_3, INTERVAL 30 DAY));
		`,
			expected: []string{
				`ALTER TABLE t1 REPLACE ROW DELETION POLICY ( OLDER_THAN ( t1_3, INTERVAL 30 DAY ))`,
			},
		},
		{
			name: "Drop ROW DELETION POLICY",
			from: `
CREATE TABLE t1 (
  t1_1 INT64 NOT NULL,
  t1_2 TIMESTAMP NOT NULL,
) PRIMARY KEY(t1_1), ROW DELETION POLICY (OLDER_THAN(t1_2, INTERVAL 30 DAY));
		`,
			to: `
CREATE TABLE t1 (
  t1_1 INT64 NOT NULL,
  t1_2 TIMESTAMP NOT NULL,
) PRIMARY KEY(t1_1);
		`,
			expected: []string{
				`ALTER TABLE t1 DROP ROW DELETION POLICY`,
			},
		},
		{
			name: "Alter database, only position's diff",
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
				`ALTER DATABASE db SET OPTIONS (optimizer_version=null, version_retention_period=null, enable_key_visualizer=null)`,
			},
		},
		{
			name: "from is empty",
			from: ``,
			to: `
ALTER DATABASE db SET OPTIONS(optimizer_version=2);
			`,
			expected: []string{
				`ALTER DATABASE db SET OPTIONS (optimizer_version=2)`,
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
				`ALTER DATABASE db SET OPTIONS (optimizer_version=2, version_retention_period='2d', enable_key_visualizer=true)`,
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
				`ALTER DATABASE db SET OPTIONS (optimizer_version=2, version_retention_period=null, enable_key_visualizer=null)`,
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
				`ALTER DATABASE db SET OPTIONS (optimizer_version=null, version_retention_period='4d', enable_key_visualizer=null)`,
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
				`ALTER DATABASE db SET OPTIONS (optimizer_version=null, version_retention_period=null, enable_key_visualizer=true)`,
			},
		},
	}
	for _, v := range values {
		t.Run(v.name, func(t *testing.T) {
			ctx := context.Background()

			d1, err := StringSource(v.from).DDL(ctx)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			d2, err := StringSource(v.to).DDL(ctx)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}

			ddl, err := hammer.Diff(d1, d2)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			actual := convertStrings(ddl)

			if !reflect.DeepEqual(actual, v.expected) {
				t.Fatalf("\ngot:\n%s,\nwant:\n%s\n", strings.Join(actual, "\n"), strings.Join(v.expected, "\n"))
			}
		})
	}
}

func convertStrings(ddl hammer.DDL) []string {
	ret := make([]string, len(ddl.List))
	for i, stmt := range ddl.List {
		ret[i] = stmt.SQL()
	}
	return ret
}
