package hammer_test

import "testing"

func TestDiffIndex(t *testing.T) {
	runDiffTests(t, []diffTestCase{
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
			name: "create unique index",
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
) PRIMARY KEY(t1_1);
CREATE UNIQUE INDEX idx_t1_2 ON t1(t1_2);
`,
			expected: []string{
				"CREATE UNIQUE INDEX idx_t1_2 ON t1(t1_2)",
			},
		},
		{
			name: "create null_filtered index",
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
) PRIMARY KEY(t1_1);
CREATE NULL_FILTERED INDEX idx_t1_2 ON t1(t1_2);
`,
			expected: []string{
				"CREATE NULL_FILTERED INDEX idx_t1_2 ON t1(t1_2)",
			},
		},
		{
			name: "add unique attribute to existing index recreates it",
			from: `
CREATE TABLE t1 (
  t1_1 INT64 NOT NULL,
  t1_2 INT64,
) PRIMARY KEY(t1_1);
CREATE INDEX idx_t1_2 ON t1(t1_2);
`,
			to: `
CREATE TABLE t1 (
  t1_1 INT64 NOT NULL,
  t1_2 INT64,
) PRIMARY KEY(t1_1);
CREATE UNIQUE INDEX idx_t1_2 ON t1(t1_2);
`,
			expected: []string{
				"DROP INDEX idx_t1_2",
				"CREATE UNIQUE INDEX idx_t1_2 ON t1(t1_2)",
			},
		},
		{
			name: "create index with DESC key",
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
) PRIMARY KEY(t1_1);
CREATE INDEX idx_t1_2 ON t1(t1_2 DESC);
`,
			expected: []string{
				"CREATE INDEX idx_t1_2 ON t1(t1_2 DESC)",
			},
		},
		{
			name: "change index key direction ASC to DESC recreates it",
			from: `
CREATE TABLE t1 (
  t1_1 INT64 NOT NULL,
  t1_2 INT64,
) PRIMARY KEY(t1_1);
CREATE INDEX idx_t1_2 ON t1(t1_2);
`,
			to: `
CREATE TABLE t1 (
  t1_1 INT64 NOT NULL,
  t1_2 INT64,
) PRIMARY KEY(t1_1);
CREATE INDEX idx_t1_2 ON t1(t1_2 DESC);
`,
			expected: []string{
				"DROP INDEX idx_t1_2",
				"CREATE INDEX idx_t1_2 ON t1(t1_2 DESC)",
			},
		},
		{
			name: "create index with INTERLEAVE IN",
			from: `
CREATE TABLE t1 (
  t1_1 INT64 NOT NULL,
) PRIMARY KEY(t1_1);
CREATE TABLE t2 (
  t1_1 INT64 NOT NULL,
  t2_1 INT64 NOT NULL,
  t2_2 INT64,
) PRIMARY KEY(t1_1, t2_1), INTERLEAVE IN PARENT t1 ON DELETE NO ACTION;
`,
			to: `
CREATE TABLE t1 (
  t1_1 INT64 NOT NULL,
) PRIMARY KEY(t1_1);
CREATE TABLE t2 (
  t1_1 INT64 NOT NULL,
  t2_1 INT64 NOT NULL,
  t2_2 INT64,
) PRIMARY KEY(t1_1, t2_1), INTERLEAVE IN PARENT t1 ON DELETE NO ACTION;
CREATE INDEX idx_t2_2 ON t2(t1_1, t2_2), INTERLEAVE IN t1;
`,
			expected: []string{
				"CREATE INDEX idx_t2_2 ON t2(t1_1, t2_2), INTERLEAVE IN t1",
			},
		},
		{
			name: "create index IF NOT EXISTS is ignored when comparing (explicit to implicit)",
			from: `
CREATE TABLE t1 (
  t1_1 INT64 NOT NULL,
  t1_2 INT64,
) PRIMARY KEY(t1_1);
CREATE INDEX IF NOT EXISTS idx_t1 ON t1(t1_2);
`,
			to: `
CREATE TABLE t1 (
  t1_1 INT64 NOT NULL,
  t1_2 INT64,
) PRIMARY KEY(t1_1);
CREATE INDEX idx_t1 ON t1(t1_2);
`,
			expected: []string{},
		},
		{
			name: "create index IF NOT EXISTS is ignored when comparing (implicit to explicit)",
			from: `
CREATE TABLE t1 (
  t1_1 INT64 NOT NULL,
  t1_2 INT64,
) PRIMARY KEY(t1_1);
CREATE INDEX idx_t1 ON t1(t1_2);
`,
			to: `
CREATE TABLE t1 (
  t1_1 INT64 NOT NULL,
  t1_2 INT64,
) PRIMARY KEY(t1_1);
CREATE INDEX IF NOT EXISTS idx_t1 ON t1(t1_2);
`,
			expected: []string{},
		},
		{
			name: "index follows STORING column drop+create",
			from: `
CREATE TABLE t1 (
  t1_1 INT64 NOT NULL,
  t1_2 INT64 NOT NULL,
  t1_3 INT64,
) PRIMARY KEY(t1_1);
CREATE INDEX idx_t1 ON t1(t1_2) STORING (t1_3);
`,
			to: `
CREATE TABLE t1 (
  t1_1 INT64 NOT NULL,
  t1_2 INT64 NOT NULL,
  t1_3 STRING(MAX),
) PRIMARY KEY(t1_1);
CREATE INDEX idx_t1 ON t1(t1_2) STORING (t1_3);
`,
			expected: []string{
				"DROP INDEX idx_t1",
				"ALTER TABLE t1 DROP COLUMN t1_3",
				"ALTER TABLE t1 ADD COLUMN t1_3 STRING(MAX)",
				"CREATE INDEX idx_t1 ON t1(t1_2) STORING (t1_3)",
			},
		},
	})
}

func TestDiffSearchIndex(t *testing.T) {
	runDiffTests(t, []diffTestCase{
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
			name: "search index with multi-column PARTITION BY",
			from: `
CREATE TABLE t1 (
  id INT64 NOT NULL,
  a INT64 NOT NULL,
  b INT64 NOT NULL,
  c STRING(MAX),
  c_tok TOKENLIST AS (TOKENIZE_FULLTEXT(c)) HIDDEN,
) PRIMARY KEY(id);
`,
			to: `
CREATE TABLE t1 (
  id INT64 NOT NULL,
  a INT64 NOT NULL,
  b INT64 NOT NULL,
  c STRING(MAX),
  c_tok TOKENLIST AS (TOKENIZE_FULLTEXT(c)) HIDDEN,
) PRIMARY KEY(id);
CREATE SEARCH INDEX idx_c ON t1(c_tok) PARTITION BY a, b;
`,
			expected: []string{
				"CREATE SEARCH INDEX idx_c ON t1(c_tok) PARTITION BY a, b",
			},
		},
		{
			name: "search index order by ASC is the default (explicit to implicit)",
			from: `
CREATE TABLE t1 (
  t1_1 INT64 NOT NULL,
  t1_2 STRING(MAX) NOT NULL,
  t1_3 TOKENLIST AS (TOKENIZE_FULLTEXT(t1_2)) HIDDEN,
) PRIMARY KEY(t1_1);
CREATE SEARCH INDEX idx_t1 ON t1(t1_3) ORDER BY t1_1 ASC;
`,
			to: `
CREATE TABLE t1 (
  t1_1 INT64 NOT NULL,
  t1_2 STRING(MAX) NOT NULL,
  t1_3 TOKENLIST AS (TOKENIZE_FULLTEXT(t1_2)) HIDDEN,
) PRIMARY KEY(t1_1);
CREATE SEARCH INDEX idx_t1 ON t1(t1_3) ORDER BY t1_1;
`,
			expected: []string{},
		},
		{
			name: "search index order by ASC is the default (implicit to explicit)",
			from: `
CREATE TABLE t1 (
  t1_1 INT64 NOT NULL,
  t1_2 STRING(MAX) NOT NULL,
  t1_3 TOKENLIST AS (TOKENIZE_FULLTEXT(t1_2)) HIDDEN,
) PRIMARY KEY(t1_1);
CREATE SEARCH INDEX idx_t1 ON t1(t1_3) ORDER BY t1_1;
`,
			to: `
CREATE TABLE t1 (
  t1_1 INT64 NOT NULL,
  t1_2 STRING(MAX) NOT NULL,
  t1_3 TOKENLIST AS (TOKENIZE_FULLTEXT(t1_2)) HIDDEN,
) PRIMARY KEY(t1_1);
CREATE SEARCH INDEX idx_t1 ON t1(t1_3) ORDER BY t1_1 ASC;
`,
			expected: []string{},
		},
		{
			name: "search index order by DESC is detected as a real change",
			from: `
CREATE TABLE t1 (
  t1_1 INT64 NOT NULL,
  t1_2 STRING(MAX) NOT NULL,
  t1_3 TOKENLIST AS (TOKENIZE_FULLTEXT(t1_2)) HIDDEN,
) PRIMARY KEY(t1_1);
CREATE SEARCH INDEX idx_t1 ON t1(t1_3) ORDER BY t1_1;
`,
			to: `
CREATE TABLE t1 (
  t1_1 INT64 NOT NULL,
  t1_2 STRING(MAX) NOT NULL,
  t1_3 TOKENLIST AS (TOKENIZE_FULLTEXT(t1_2)) HIDDEN,
) PRIMARY KEY(t1_1);
CREATE SEARCH INDEX idx_t1 ON t1(t1_3) ORDER BY t1_1 DESC;
`,
			expected: []string{
				"DROP SEARCH INDEX idx_t1",
				"CREATE SEARCH INDEX idx_t1 ON t1(t1_3) ORDER BY t1_1 DESC",
			},
		},
		{
			name: "search index follows STORING column drop+create",
			from: `
CREATE TABLE t1 (
  t1_1 INT64 NOT NULL,
  t1_2 STRING(MAX) NOT NULL,
  t1_3 TOKENLIST AS (TOKENIZE_FULLTEXT(t1_2)) HIDDEN,
  t1_4 INT64,
) PRIMARY KEY(t1_1);
CREATE SEARCH INDEX idx_t1 ON t1(t1_3) STORING (t1_4);
`,
			to: `
CREATE TABLE t1 (
  t1_1 INT64 NOT NULL,
  t1_2 STRING(MAX) NOT NULL,
  t1_3 TOKENLIST AS (TOKENIZE_FULLTEXT(t1_2)) HIDDEN,
  t1_4 STRING(MAX),
) PRIMARY KEY(t1_1);
CREATE SEARCH INDEX idx_t1 ON t1(t1_3) STORING (t1_4);
`,
			expected: []string{
				"DROP SEARCH INDEX idx_t1",
				"ALTER TABLE t1 DROP COLUMN t1_4",
				"ALTER TABLE t1 ADD COLUMN t1_4 STRING(MAX)",
				"CREATE SEARCH INDEX idx_t1 ON t1(t1_3) STORING (t1_4)",
			},
		},
		{
			name: "search index follows PARTITION BY column drop+create",
			from: `
CREATE TABLE t1 (
  t1_1 INT64 NOT NULL,
  t1_2 STRING(MAX) NOT NULL,
  t1_3 TOKENLIST AS (TOKENIZE_FULLTEXT(t1_2)) HIDDEN,
  t1_4 INT64 NOT NULL,
) PRIMARY KEY(t1_1);
CREATE SEARCH INDEX idx_t1 ON t1(t1_3) PARTITION BY t1_4;
`,
			to: `
CREATE TABLE t1 (
  t1_1 INT64 NOT NULL,
  t1_2 STRING(MAX) NOT NULL,
  t1_3 TOKENLIST AS (TOKENIZE_FULLTEXT(t1_2)) HIDDEN,
  t1_4 STRING(MAX) NOT NULL,
) PRIMARY KEY(t1_1);
CREATE SEARCH INDEX idx_t1 ON t1(t1_3) PARTITION BY t1_4;
`,
			expected: []string{
				"DROP SEARCH INDEX idx_t1",
				"ALTER TABLE t1 DROP COLUMN t1_4",
				`ALTER TABLE t1 ADD COLUMN t1_4 STRING(MAX) NOT NULL DEFAULT ("")`,
				"ALTER TABLE t1 ALTER COLUMN t1_4 DROP DEFAULT",
				"CREATE SEARCH INDEX idx_t1 ON t1(t1_3) PARTITION BY t1_4",
			},
		},
		{
			name: "search index follows ORDER BY column drop+create",
			from: `
CREATE TABLE t1 (
  t1_1 INT64 NOT NULL,
  t1_2 STRING(MAX) NOT NULL,
  t1_3 TOKENLIST AS (TOKENIZE_FULLTEXT(t1_2)) HIDDEN,
  t1_4 INT64 NOT NULL,
) PRIMARY KEY(t1_1);
CREATE SEARCH INDEX idx_t1 ON t1(t1_3) ORDER BY t1_4 DESC;
`,
			to: `
CREATE TABLE t1 (
  t1_1 INT64 NOT NULL,
  t1_2 STRING(MAX) NOT NULL,
  t1_3 TOKENLIST AS (TOKENIZE_FULLTEXT(t1_2)) HIDDEN,
  t1_4 STRING(MAX) NOT NULL,
) PRIMARY KEY(t1_1);
CREATE SEARCH INDEX idx_t1 ON t1(t1_3) ORDER BY t1_4 DESC;
`,
			expected: []string{
				"DROP SEARCH INDEX idx_t1",
				"ALTER TABLE t1 DROP COLUMN t1_4",
				`ALTER TABLE t1 ADD COLUMN t1_4 STRING(MAX) NOT NULL DEFAULT ("")`,
				"ALTER TABLE t1 ALTER COLUMN t1_4 DROP DEFAULT",
				"CREATE SEARCH INDEX idx_t1 ON t1(t1_3) ORDER BY t1_4 DESC",
			},
		},
	})
}
