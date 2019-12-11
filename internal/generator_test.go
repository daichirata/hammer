package internal_test

import (
	"reflect"
	"testing"

	"github.com/daichirata/hammer/internal"
)

type StringSource string

func (s StringSource) String() string        { return string(s) }
func (s StringSource) Read() (string, error) { return string(s), nil }

func TestGenerateDDLs(t *testing.T) {
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
		// drop column
		{
			from: `
CREATE TABLE t1 (
  t1_1 INT64 NOT NULL,
  t1_2 INT64 NOT NULL,
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
		// drop index
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
	}
	for _, v := range values {
		ddls, err := internal.GenerateDDLs(StringSource(v.from), StringSource(v.to))
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		actual := convertStrings(ddls)

		if !reflect.DeepEqual(actual, v.expected) {
			t.Fatalf("got: %v, want: %v", actual, v.expected)
		}
	}
}

func convertStrings(ddls []internal.DDL) []string {
	ret := make([]string, len(ddls))
	for i, d := range ddls {
		ret[i] = d.SQL()
	}
	return ret
}
