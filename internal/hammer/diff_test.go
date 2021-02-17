package hammer_test

import (
	"context"
	"fmt"
	"reflect"
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
		from     string
		to       string
		expected []string
	}{
		// drop table
		{
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
		// create table
		{
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
		// drop column (different column positions)
		{
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
		// add column (allow null)
		{
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
		// add column (not null)
		{
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
		// change column (different type)
		{
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
		// change column (same type)
		{
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
		// change column (timestamp)
		{
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
				`UPDATE t1 SET t1_2 = '0001-01-01 00:00:00' WHERE t1_2 IS NULL`,
				`ALTER TABLE t1 ALTER COLUMN t1_2 TIMESTAMP NOT NULL`,
			},
		},
		{
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
				`UPDATE t1 SET t1_2 = '0001-01-01 00:00:00' WHERE t1_2 IS NULL`,
				`ALTER TABLE t1 ALTER COLUMN t1_2 TIMESTAMP NOT NULL`,
				`ALTER TABLE t1 ALTER COLUMN t1_2 SET OPTIONS (allow_commit_timestamp = true)`,
			},
		},
		{
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
		// add index
		{
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
		// drop index (different index positions)
		{
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
		// change indexed column
		{
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
		// change column (interleaved)
		{
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
		// Create table with constraint
		{
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
		// Add named constraint
		{
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
		// Add unnamed constraint
		{
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
		// Update named constraint
		{
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
		// Update unnamed constraint
		{
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
		// Drop named constraint
		{
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
		// Drop unnamed constraint
		{
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
		// Update constraint referencing new column
		{
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
		// Drop constraint referencing dropped column.
		{
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
		// Drop constraint referencing dropped table.
		{
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
		// Drop multiple named constraint referencing dropped column.
		{
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
		// Drop named constraint referencing multiple dropped columns.
		{
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
		// Update constraint referencing dropped column
		{
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
		// Recreate constraint if recreating referenced table.
		{
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
		// AlterTable add foreign key
		{
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
	}
	for i, v := range values {
		t.Run(fmt.Sprintf("%d", i), func(t *testing.T) {
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
				t.Fatalf("got: %v, want: %v", actual, v.expected)
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
