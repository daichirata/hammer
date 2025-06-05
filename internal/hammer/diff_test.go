package hammer_test

import (
	"context"
	"testing"

	"github.com/google/go-cmp/cmp"

	"github.com/daichirata/hammer/internal/hammer"
)

type StringSource string

func (s StringSource) String() string { return string(s) }
func (s StringSource) DDL(_ context.Context, o *hammer.DDLOption) (hammer.DDL, error) {
	return hammer.ParseDDL("string", s.String(), o)
}

func TestDiff(t *testing.T) {
	values := []struct {
		name                string
		from                string
		to                  string
		ignoreAlterDatabase bool
		expected            []string
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
  t2_1 INT64 NOT NULL
) PRIMARY KEY (t2_1)`,
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
  t1_2 BOOL NOT NULL,
  t1_3 INT64 NOT NULL,
  t1_4 FLOAT64 NOT NULL,
  t1_5 STRING(MAX) NOT NULL,
  t1_6 BYTES(MAX) NOT NULL,
  t1_7 DATE NOT NULL,
  t1_8 TIMESTAMP NOT NULL,
  t1_9 JSON NOT NULL,
  t1_10 ARRAY<INT64> NOT NULL,
  t1_11 NUMERIC NOT NULL,
) PRIMARY KEY(t1_1);
`,
			expected: []string{
				`ALTER TABLE t1 ADD COLUMN t1_2 BOOL NOT NULL DEFAULT (FALSE)`,
				`ALTER TABLE t1 ALTER COLUMN t1_2 DROP DEFAULT`,
				`ALTER TABLE t1 ADD COLUMN t1_3 INT64 NOT NULL DEFAULT (0)`,
				`ALTER TABLE t1 ALTER COLUMN t1_3 DROP DEFAULT`,
				`ALTER TABLE t1 ADD COLUMN t1_4 FLOAT64 NOT NULL DEFAULT (0)`,
				`ALTER TABLE t1 ALTER COLUMN t1_4 DROP DEFAULT`,
				`ALTER TABLE t1 ADD COLUMN t1_5 STRING(MAX) NOT NULL DEFAULT ("")`,
				`ALTER TABLE t1 ALTER COLUMN t1_5 DROP DEFAULT`,
				`ALTER TABLE t1 ADD COLUMN t1_6 BYTES(MAX) NOT NULL DEFAULT (b"")`,
				`ALTER TABLE t1 ALTER COLUMN t1_6 DROP DEFAULT`,
				`ALTER TABLE t1 ADD COLUMN t1_7 DATE NOT NULL DEFAULT (DATE "0001-01-01")`,
				`ALTER TABLE t1 ALTER COLUMN t1_7 DROP DEFAULT`,
				`ALTER TABLE t1 ADD COLUMN t1_8 TIMESTAMP NOT NULL DEFAULT (TIMESTAMP "0001-01-01T00:00:00Z")`,
				`ALTER TABLE t1 ALTER COLUMN t1_8 DROP DEFAULT`,
				`ALTER TABLE t1 ADD COLUMN t1_9 JSON NOT NULL DEFAULT (JSON "{}")`,
				`ALTER TABLE t1 ALTER COLUMN t1_9 DROP DEFAULT`,
				`ALTER TABLE t1 ADD COLUMN t1_10 ARRAY<INT64> NOT NULL DEFAULT (ARRAY[])`,
				`ALTER TABLE t1 ALTER COLUMN t1_10 DROP DEFAULT`,
				`ALTER TABLE t1 ADD COLUMN t1_11 NUMERIC NOT NULL DEFAULT (NUMERIC "0")`,
				`ALTER TABLE t1 ALTER COLUMN t1_11 DROP DEFAULT`,
			},
		},
		{
			name: "add column (default)",
			from: `
CREATE TABLE t1 (
  t1_1 INT64 DEFAULT (1),
) PRIMARY KEY(t1_1);
`,
			to: `
CREATE TABLE t1 (
  t1_1 INT64 DEFAULT (1),
  t1_2 BOOL DEFAULT (TRUE),
  t1_3 INT64 DEFAULT (2),
  t1_4 FLOAT64 DEFAULT (3),
  t1_5 STRING(MAX) DEFAULT ("default"),
  t1_6 BYTES(MAX) DEFAULT (b"default"),
  t1_7 DATE DEFAULT (DATE '2022-06-18'),
  t1_8 TIMESTAMP DEFAULT (TIMESTAMP '2022-06-18 04:36:00.000000+09:00'),
  t1_9 JSON DEFAULT (JSON '{"key": "value"}'),
  t1_10 ARRAY<INT64> DEFAULT ([1]),
  t1_11 NUMERIC DEFAULT (11),
) PRIMARY KEY(t1_1);
`,
			expected: []string{
				`ALTER TABLE t1 ADD COLUMN t1_2 BOOL DEFAULT (TRUE)`,
				`ALTER TABLE t1 ADD COLUMN t1_3 INT64 DEFAULT (2)`,
				`ALTER TABLE t1 ADD COLUMN t1_4 FLOAT64 DEFAULT (3)`,
				`ALTER TABLE t1 ADD COLUMN t1_5 STRING(MAX) DEFAULT ("default")`,
				`ALTER TABLE t1 ADD COLUMN t1_6 BYTES(MAX) DEFAULT (b"default")`,
				`ALTER TABLE t1 ADD COLUMN t1_7 DATE DEFAULT (DATE "2022-06-18")`,
				`ALTER TABLE t1 ADD COLUMN t1_8 TIMESTAMP DEFAULT (TIMESTAMP "2022-06-18 04:36:00.000000+09:00")`,
				`ALTER TABLE t1 ADD COLUMN t1_9 JSON DEFAULT (JSON '{"key": "value"}')`,
				`ALTER TABLE t1 ADD COLUMN t1_10 ARRAY<INT64> DEFAULT ([1])`,
				`ALTER TABLE t1 ADD COLUMN t1_11 NUMERIC DEFAULT (11)`,
			},
		},
		{
			name: "add column (not null default)",
			from: `
CREATE TABLE t1 (
  t1_1 INT64 NOT NULL DEFAULT (1),
) PRIMARY KEY(t1_1);
`,
			to: `
CREATE TABLE t1 (
  t1_1 INT64 NOT NULL DEFAULT (1),
  t1_2 BOOL NOT NULL DEFAULT (TRUE),
  t1_3 INT64 NOT NULL DEFAULT (2),
  t1_4 FLOAT64 NOT NULL DEFAULT (3),
  t1_5 STRING(MAX) NOT NULL DEFAULT ("default"),
  t1_6 BYTES(MAX) NOT NULL DEFAULT (b"default"),
  t1_7 DATE NOT NULL DEFAULT (DATE '2022-06-18'),
  t1_8 TIMESTAMP NOT NULL DEFAULT (TIMESTAMP '2022-06-18 04:36:00.000000+09:00'),
  t1_9 JSON NOT NULL DEFAULT (JSON '{"key": "value"}'),
  t1_10 ARRAY<INT64> NOT NULL DEFAULT ([1]),
  t1_11 NUMERIC NOT NULL DEFAULT (11),
) PRIMARY KEY(t1_1);
`,
			expected: []string{
				`ALTER TABLE t1 ADD COLUMN t1_2 BOOL NOT NULL DEFAULT (TRUE)`,
				`ALTER TABLE t1 ADD COLUMN t1_3 INT64 NOT NULL DEFAULT (2)`,
				`ALTER TABLE t1 ADD COLUMN t1_4 FLOAT64 NOT NULL DEFAULT (3)`,
				`ALTER TABLE t1 ADD COLUMN t1_5 STRING(MAX) NOT NULL DEFAULT ("default")`,
				`ALTER TABLE t1 ADD COLUMN t1_6 BYTES(MAX) NOT NULL DEFAULT (b"default")`,
				`ALTER TABLE t1 ADD COLUMN t1_7 DATE NOT NULL DEFAULT (DATE "2022-06-18")`,
				`ALTER TABLE t1 ADD COLUMN t1_8 TIMESTAMP NOT NULL DEFAULT (TIMESTAMP "2022-06-18 04:36:00.000000+09:00")`,
				`ALTER TABLE t1 ADD COLUMN t1_9 JSON NOT NULL DEFAULT (JSON '{"key": "value"}')`,
				`ALTER TABLE t1 ADD COLUMN t1_10 ARRAY<INT64> NOT NULL DEFAULT ([1])`,
				`ALTER TABLE t1 ADD COLUMN t1_11 NUMERIC NOT NULL DEFAULT (11)`,
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
			name: "set NOT NULL to timestamp column",
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
				`UPDATE t1 SET t1_2 = TIMESTAMP "0001-01-01T00:00:00Z" WHERE t1_2 IS NULL`,
				`ALTER TABLE t1 ALTER COLUMN t1_2 TIMESTAMP NOT NULL`,
			},
		},
		{
			name: "set NOT NULL to string column",
			from: `
CREATE TABLE t1 (
  t1_1 INT64 NOT NULL,
  t1_2 STRING(MAX),
) PRIMARY KEY(t1_1);
`,
			to: `
CREATE TABLE t1 (
  t1_1 INT64 NOT NULL,
  t1_2 STRING(MAX) NOT NULL,
) PRIMARY KEY(t1_1);
`,
			expected: []string{
				`UPDATE t1 SET t1_2 = "" WHERE t1_2 IS NULL`,
				`ALTER TABLE t1 ALTER COLUMN t1_2 STRING(MAX) NOT NULL`,
			},
		},
		{
			name: "set DEFAULT",
			from: `
CREATE TABLE t1 (
  t1_1 INT64 NOT NULL,
  t1_2 STRING(MAX) NOT NULL,
) PRIMARY KEY(t1_1);
`,
			to: `
CREATE TABLE t1 (
  t1_1 INT64 NOT NULL,
  t1_2 STRING(MAX) NOT NULL DEFAULT ("default value"),
) PRIMARY KEY(t1_1);
`,
			expected: []string{
				`ALTER TABLE t1 ALTER COLUMN t1_2 STRING(MAX) NOT NULL DEFAULT ("default value")`,
			},
		},
		{
			name: "set NOT NULL and DEFAULT",
			from: `
CREATE TABLE t1 (
  t1_1 INT64 NOT NULL,
  t1_2 STRING(MAX),
) PRIMARY KEY(t1_1);
`,
			to: `
CREATE TABLE t1 (
  t1_1 INT64 NOT NULL,
  t1_2 STRING(MAX) NOT NULL DEFAULT ("default value"),
) PRIMARY KEY(t1_1);
`,
			expected: []string{
				`UPDATE t1 SET t1_2 = "default value" WHERE t1_2 IS NULL`,
				`ALTER TABLE t1 ALTER COLUMN t1_2 STRING(MAX) NOT NULL DEFAULT ("default value")`,
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
				`UPDATE t1 SET t1_2 = TIMESTAMP "0001-01-01T00:00:00Z" WHERE t1_2 IS NULL`,
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
			name: "add hidden column",
			from: `
CREATE TABLE t1 (
  t1_1 STRING(36) NOT NULL,
) PRIMARY KEY(t1_1);
`,
			to: `
CREATE TABLE t1 (
  t1_1 STRING(36) NOT NULL,
  t1_2 TOKENLIST AS (TOKENIZE_NUMBER(Value, comparison_type => "all", min => 1, max => 5)) HIDDEN,
) PRIMARY KEY(t1_1);
`,
			expected: []string{
				`ALTER TABLE t1 ADD COLUMN t1_2 TOKENLIST AS (TOKENIZE_NUMBER(Value, comparison_type => "all", min => 1, max => 5)) HIDDEN`,
			},
		},
		{
			name: "add hidden column different pos",
			from: `
CREATE TABLE t1 (
  t1_1 STRING(36) NOT NULL,
  t1_2 TOKENLIST AS (TOKENIZE_NUMBER(Value, comparison_type => "all", min => 1, max => 5)) HIDDEN,
) PRIMARY KEY(t1_1);
`,
			to: `
CREATE TABLE t1 (
  t1_1 STRING(36) NOT NULL, t1_2 TOKENLIST AS (TOKENIZE_NUMBER(Value, comparison_type => "all", min => 1, max => 5)) HIDDEN,
) PRIMARY KEY(t1_1);
`,
			expected: []string{},
		},
		{
			name: "add hidden attribute to column",
			from: `
CREATE TABLE t1 (
  t1_1 STRING(36) NOT NULL,
  t1_2 STRING(36) NOT NULL,
) PRIMARY KEY(t1_1);
`,
			to: `
CREATE TABLE t1 (
  t1_1 STRING(36) NOT NULL,
  t1_2 STRING(36) NOT NULL HIDDEN,
) PRIMARY KEY(t1_1);
`,
			expected: []string{
				`ALTER TABLE t1 ALTER COLUMN t1_2 STRING(36) NOT NULL HIDDEN`,
			},
		},
		{
			name: "remove hidden attribute from column",
			from: `
CREATE TABLE t1 (
  t1_1 STRING(36) NOT NULL,
  t1_2 STRING(36) NOT NULL HIDDEN,
) PRIMARY KEY(t1_1);
`,
			to: `
CREATE TABLE t1 (
  t1_1 STRING(36) NOT NULL,
  t1_2 STRING(36) NOT NULL,
) PRIMARY KEY(t1_1);
`,
			expected: []string{
				`ALTER TABLE t1 ALTER COLUMN t1_2 STRING(36) NOT NULL`,
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
				`ALTER TABLE t1 ADD COLUMN t1_2 STRING(1) NOT NULL DEFAULT ("")`,
				`ALTER TABLE t1 ALTER COLUMN t1_2 DROP DEFAULT`,
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
			name: "alter index add stored column",
			from: `
CREATE TABLE t1 (
  t1_1 INT64 NOT NULL,
  t1_2 STRING(36) NOT NULL,
  t1_3 STRING(36) NOT NULL,
  t1_4 STRING(36) NOT NULL,
) PRIMARY KEY(t1_1);
CREATE INDEX idx_t1_1 ON t1(t1_2);
CREATE INDEX idx_t1_2 ON t1(t1_2) STORING (t1_3);
CREATE INDEX idx_t1_3 ON t1(t1_2);
`,
			to: `
CREATE TABLE t1 (
  t1_1 INT64 NOT NULL,
  t1_2 STRING(36) NOT NULL,
  t1_3 STRING(36) NOT NULL,
  t1_4 STRING(36) NOT NULL,
) PRIMARY KEY(t1_1);
CREATE INDEX idx_t1_1 ON t1(t1_2) STORING (t1_3);
CREATE INDEX idx_t1_2 ON t1(t1_2) STORING (t1_3, t1_4);
CREATE INDEX idx_t1_3 ON t1(t1_2) STORING (t1_3, t1_4);
`,
			expected: []string{
				`ALTER INDEX idx_t1_1 ADD STORED COLUMN t1_3`,
				`ALTER INDEX idx_t1_2 ADD STORED COLUMN t1_4`,
				`ALTER INDEX idx_t1_3 ADD STORED COLUMN t1_3`,
				`ALTER INDEX idx_t1_3 ADD STORED COLUMN t1_4`,
			},
		},
		{
			name: "alter index drop stored column",
			from: `
CREATE TABLE t1 (
  t1_1 INT64 NOT NULL,
  t1_2 STRING(36) NOT NULL,
  t1_3 STRING(36) NOT NULL,
  t1_4 STRING(36) NOT NULL,
) PRIMARY KEY(t1_1);
CREATE INDEX idx_t1_1 ON t1(t1_2) STORING (t1_3);
CREATE INDEX idx_t1_2 ON t1(t1_2) STORING (t1_3, t1_4);
CREATE INDEX idx_t1_3 ON t1(t1_2) STORING (t1_3, t1_4);
`,
			to: `
CREATE TABLE t1 (
  t1_1 INT64 NOT NULL,
  t1_2 STRING(36) NOT NULL,
  t1_3 STRING(36) NOT NULL,
  t1_4 STRING(36) NOT NULL,
) PRIMARY KEY(t1_1);
CREATE INDEX idx_t1_1 ON t1(t1_2);
CREATE INDEX idx_t1_2 ON t1(t1_2) STORING (t1_3);
CREATE INDEX idx_t1_3 ON t1(t1_2);
`,
			expected: []string{
				`ALTER INDEX idx_t1_1 DROP STORED COLUMN t1_3`,
				`ALTER INDEX idx_t1_2 DROP STORED COLUMN t1_4`,
				`ALTER INDEX idx_t1_3 DROP STORED COLUMN t1_3`,
				`ALTER INDEX idx_t1_3 DROP STORED COLUMN t1_4`,
			},
		},
		{
			name: "alter index change stored column",
			from: `
CREATE TABLE t1 (
  t1_1 INT64 NOT NULL,
  t1_2 STRING(36) NOT NULL,
  t1_3 STRING(36) NOT NULL,
  t1_4 STRING(36) NOT NULL,
) PRIMARY KEY(t1_1);
CREATE INDEX idx_t1_1 ON t1(t1_2) STORING (t1_3);
`,
			to: `
CREATE TABLE t1 (
  t1_1 INT64 NOT NULL,
  t1_2 STRING(36) NOT NULL,
  t1_3 STRING(36) NOT NULL,
  t1_4 STRING(36) NOT NULL,
) PRIMARY KEY(t1_1);
CREATE INDEX idx_t1_1 ON t1(t1_2) STORING (t1_4);
`,
			expected: []string{
				`ALTER INDEX idx_t1_1 ADD STORED COLUMN t1_4`,
				`ALTER INDEX idx_t1_1 DROP STORED COLUMN t1_3`,
			},
		},
		{
			name: "change index columns",
			from: `
CREATE TABLE t1 (
  t1_1 INT64 NOT NULL,
  t1_2 STRING(36) NOT NULL,
  t1_3 STRING(36) NOT NULL,
) PRIMARY KEY(t1_1);
CREATE INDEX idx_t1_1 ON t1(t1_2) STORING (t1_3);
`,
			to: `
CREATE TABLE t1 (
  t1_1 INT64 NOT NULL,
  t1_2 STRING(36) NOT NULL,
  t1_3 STRING(36) NOT NULL,
) PRIMARY KEY(t1_1);
CREATE INDEX idx_t1_1 ON t1(t1_3) STORING (t1_2);
`,
			expected: []string{
				`DROP INDEX idx_t1_1`,
				`CREATE INDEX idx_t1_1 ON t1(t1_3) STORING (t1_2)`,
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
				`ALTER TABLE t1 ADD COLUMN t1_3 INT64 NOT NULL DEFAULT (0)`,
				`ALTER TABLE t1 ALTER COLUMN t1_3 DROP DEFAULT`,
				`CREATE INDEX idx_t1_2 ON t1(t1_3)`,
			},
		},
		{
			name: "add search index",
			from: `
CREATE TABLE t1 (
	t1_1 STRING(MAX) NOT NULL,
	t1_2 INT64 NOT NULL,
	t1_3 TOKENLIST AS (TOKENIZE_FULLTEXT(Name)) HIDDEN,
) PRIMARY KEY(t1_1);
CREATE INDEX idx_t1_1 ON t1(t1_2);
`,
			to: `
CREATE TABLE t1 (
	t1_1 STRING(MAX) NOT NULL,
	t1_2 INT64 NOT NULL,
	t1_3 TOKENLIST AS (TOKENIZE_FULLTEXT(Name)) HIDDEN,
) PRIMARY KEY(t1_1);
CREATE INDEX idx_t1_1 ON t1(t1_2);
CREATE SEARCH INDEX idx_t1_3 ON t1(t1_3);
`,
			expected: []string{
				`CREATE SEARCH INDEX idx_t1_3 ON t1(t1_3)`,
			},
		},
		{
			name: "add advanced search index",
			from: `
CREATE TABLE t1 (
	t1_1 STRING(MAX) NOT NULL,
	t1_2 INT64 NOT NULL,
	t1_3 TOKENLIST AS (TOKENIZE_SUBSTRING(Name)) HIDDEN,
	t1_4 INT64 NOT NULL,
) PRIMARY KEY(t1_1);
`,
			to: `
CREATE TABLE t1 (
	t1_1 STRING(MAX) NOT NULL,
	t1_2 INT64 NOT NULL,
	t1_3 TOKENLIST AS (TOKENIZE_SUBSTRING(Name)) HIDDEN,
	t1_4 INT64 NOT NULL,
) PRIMARY KEY(t1_1);
CREATE SEARCH INDEX idx_t1_3 ON t1(t1_3) STORING (t1_4)
PARTITION BY t1_4
ORDER BY t1_1 DESC, INTERLEAVE IN (t1)
OPTIONS (sort_order_sharding=true);
`,
			expected: []string{
				`CREATE SEARCH INDEX idx_t1_3 ON t1(t1_3) STORING (t1_4) PARTITION BY t1_4 ORDER BY t1_1 DESC, INTERLEAVE IN (t1) OPTIONS (sort_order_sharding = true)`,
			},
		},
		{
			name: "alter search index",
			from: `
CREATE TABLE t1 (
	t1_1 STRING(MAX) NOT NULL,
	t1_2 TOKENLIST AS (TOKENIZE_FULLTEXT(Name)) HIDDEN,
	t1_3 STRING(MAX) NOT NULL,
	t1_4 TOKENLIST AS (TOKENIZE_FULLTEXT(Name)) HIDDEN,
) PRIMARY KEY(t1_1);
CREATE SEARCH INDEX idx_t1_2 ON t1(t1_2);
`,
			to: `
CREATE TABLE t1 (
	t1_1 STRING(MAX) NOT NULL,
	t1_2 TOKENLIST AS (TOKENIZE_FULLTEXT(Name)) HIDDEN,
	t1_3 STRING(MAX) NOT NULL,
	t1_4 TOKENLIST AS (TOKENIZE_FULLTEXT(Name)) HIDDEN,
) PRIMARY KEY(t1_1);
CREATE SEARCH INDEX idx_t1_2 ON t1(t1_2, t1_4);
`,
			expected: []string{
				`DROP SEARCH INDEX idx_t1_2`,
				`CREATE SEARCH INDEX idx_t1_2 ON t1(t1_2, t1_4)`,
			},
		},
		{
			name: "drop search index",
			from: `
CREATE TABLE t1 (
	t1_1 STRING(MAX) NOT NULL,
	t1_2 TOKENLIST AS (TOKENIZE_FULLTEXT(Name)) HIDDEN,
	t1_3 STRING(MAX) NOT NULL,
	t1_4 TOKENLIST AS (TOKENIZE_FULLTEXT(Name)) HIDDEN,
) PRIMARY KEY(t1_1);
CREATE SEARCH INDEX idx_t1_2 ON t1(t1_2);
`,
			to: `
CREATE TABLE t1 (
	t1_1 STRING(MAX) NOT NULL,
	t1_2 TOKENLIST AS (TOKENIZE_FULLTEXT(Name)) HIDDEN,
	t1_3 STRING(MAX) NOT NULL,
	t1_4 TOKENLIST AS (TOKENIZE_FULLTEXT(Name)) HIDDEN,
) PRIMARY KEY(t1_1);
`,
			expected: []string{
				`DROP SEARCH INDEX idx_t1_2`,
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
  t1_1 STRING(36) NOT NULL
) PRIMARY KEY (t1_1)`,
				`CREATE TABLE t2 (
  t1_1 INT64 NOT NULL,
  t2_1 INT64 NOT NULL,
  t2_2 INT64 NOT NULL,
  t2_3 INT64 NOT NULL
) PRIMARY KEY (t1_1, t2_1),
  INTERLEAVE IN PARENT t1 ON DELETE NO ACTION`,
				`CREATE INDEX idx_t2 ON t2(t2_1)`,
				`CREATE TABLE t3 (
  t1_1 INT64 NOT NULL,
  t3_1 INT64 NOT NULL
) PRIMARY KEY (t1_1, t3_1),
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
  CONSTRAINT FK_t2_1 FOREIGN KEY (t2_1) REFERENCES t1 (t1_1)
) PRIMARY KEY (t2_1)`,
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
  t1_2 INT64
) PRIMARY KEY (t1_2)`,
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
  t1_2 TIMESTAMP NOT NULL
) PRIMARY KEY (t1_1), ROW DELETION POLICY ( OLDER_THAN ( t1_2, INTERVAL 30 DAY ))`,
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
				`ALTER DATABASE db SET OPTIONS (optimizer_version = null, version_retention_period = null, enable_key_visualizer = null)`,
			},
		},
		{
			name: "from is empty",
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
				`CREATE TABLE Singers (
  id INT64 NOT NULL
) PRIMARY KEY (id)`,
				`CREATE TABLE Albums (
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
			expected:            []string{"DROP CHANGE STREAM SomeStream", "CREATE CHANGE STREAM SomeStream"},
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
				`CREATE TABLE Albums (
  id INT64 NOT NULL
) PRIMARY KEY (id)`,
				"DROP CHANGE STREAM SomeStream",
				"DROP TABLE Singers",
				`CREATE CHANGE STREAM SomeStream FOR Albums`,
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
CREATE CHANGE STREAM SomeStream FOR ALL OPTIONS( retention_period = '36h', value_capture_type = 'NEW_VALUES' );
`,
			to: `
CREATE CHANGE STREAM SomeStream FOR ALL OPTIONS( retention_period = '5d', value_capture_type = 'NEW_ROW' );
`,
			ignoreAlterDatabase: true,
			expected:            []string{`ALTER CHANGE STREAM SomeStream SET OPTIONS (retention_period = "5d", value_capture_type = "NEW_ROW")`},
		},
		{
			name: "alter change stream option to default",
			from: `
CREATE CHANGE STREAM SomeStream FOR ALL OPTIONS( retention_period = '36h', value_capture_type = 'NEW_VALUES' );
`,
			to: `
CREATE CHANGE STREAM SomeStream FOR ALL;
`,
			ignoreAlterDatabase: true,
			expected:            []string{`ALTER CHANGE STREAM SomeStream SET OPTIONS (retention_period = "1d", value_capture_type = "OLD_AND_NEW_VALUES")`},
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
			name: "drop view",
			from: `
CREATE TABLE t1 (
	t1_1 INT64 NOT NULL,
) PRIMARY KEY(t1_1);

CREATE VIEW v1 
SQL SECURITY INVOKER
AS SELECT * FROM t1;
`,
			to: `
CREATE TABLE t1 (
	t1_1 INT64 NOT NULL,
) PRIMARY KEY(t1_1);
`,
			expected: []string{
				`DROP VIEW v1`,
			},
		},
		{
			name: "create view",
			from: `
CREATE TABLE t1 (
	t1_1 INT64 NOT NULL,
) PRIMARY KEY(t1_1);
`,
			to: `
CREATE TABLE t1 (
	t1_1 INT64 NOT NULL,
) PRIMARY KEY(t1_1);

CREATE VIEW v1 
SQL SECURITY INVOKER
AS SELECT * FROM t1;
`,
			expected: []string{
				`CREATE VIEW v1 SQL SECURITY INVOKER AS SELECT * FROM t1`,
			},
		},
		{
			name: "replace view",
			from: `
CREATE TABLE t1 (
	t1_1 INT64 NOT NULL,
) PRIMARY KEY(t1_1);

CREATE VIEW v1 
SQL SECURITY INVOKER
AS SELECT * FROM t1 WHERE t1_1 > 0;
`,
			to: `
CREATE TABLE t1 (
	t1_1 INT64 NOT NULL,
) PRIMARY KEY(t1_1);

CREATE VIEW v1 
SQL SECURITY INVOKER
AS SELECT * FROM t1;
`,
			expected: []string{
				`CREATE OR REPLACE VIEW v1 SQL SECURITY INVOKER AS SELECT * FROM t1`,
			},
		},
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
	}
	for _, v := range values {
		t.Run(v.name, func(t *testing.T) {
			ctx := context.Background()

			d1, err := StringSource(v.from).DDL(ctx, &hammer.DDLOption{IgnoreAlterDatabase: v.ignoreAlterDatabase})
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			d2, err := StringSource(v.to).DDL(ctx, &hammer.DDLOption{IgnoreAlterDatabase: v.ignoreAlterDatabase})
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}

			ddl, err := hammer.Diff(d1, d2)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			actual := convertStrings(ddl)
			if diff := cmp.Diff(v.expected, actual); diff != "" {
				t.Errorf("(-want, +got)\n%s", diff)
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
