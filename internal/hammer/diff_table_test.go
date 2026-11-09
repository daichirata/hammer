package hammer_test

import "testing"

func TestDiffTable(t *testing.T) {
	runDiffTests(t, []diffTestCase{
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
				`
CREATE TABLE t2 (
  t2_1 INT64 NOT NULL
) PRIMARY KEY (t2_1)`,
			},
		},
		{
			name: "create table with composite primary key mixed direction",
			from: `
CREATE TABLE other (
  id INT64 NOT NULL,
) PRIMARY KEY(id);`,
			to: `
CREATE TABLE other (
  id INT64 NOT NULL,
) PRIMARY KEY(id);

CREATE TABLE t1 (
  a INT64 NOT NULL,
  b INT64 NOT NULL,
) PRIMARY KEY(a, b DESC);
`,
			expected: []string{
				`
CREATE TABLE t1 (
  a INT64 NOT NULL,
  b INT64 NOT NULL
) PRIMARY KEY (a, b DESC)`,
			},
		},
		{
			name: "change PK column direction triggers table recreation",
			from: `
CREATE TABLE t1 (
  a INT64 NOT NULL,
  b INT64 NOT NULL,
) PRIMARY KEY(a, b);
`,
			to: `
CREATE TABLE t1 (
  a INT64 NOT NULL,
  b INT64 NOT NULL,
) PRIMARY KEY(a, b DESC);
`,
			expected: []string{
				"DROP TABLE t1",
				`
CREATE TABLE t1 (
  a INT64 NOT NULL,
  b INT64 NOT NULL
) PRIMARY KEY (a, b DESC)`,
			},
		},
		{
			name: "single-column inline primary key is semantically equivalent to table primary key clause",
			from: `
CREATE TABLE t1 (
  t1_1 INT64 NOT NULL,
) PRIMARY KEY(t1_1);
`,
			to: `
CREATE TABLE t1 (
  t1_1 INT64 NOT NULL PRIMARY KEY,
);
`,
			expected: []string{},
		},
		{
			name: "create table IF NOT EXISTS is ignored when comparing (explicit to implicit)",
			from: `
CREATE TABLE IF NOT EXISTS t1 (
  t1_1 INT64 NOT NULL,
) PRIMARY KEY(t1_1);
`,
			to: `
CREATE TABLE t1 (
  t1_1 INT64 NOT NULL,
) PRIMARY KEY(t1_1);
`,
			expected: []string{},
		},
		{
			name: "create table IF NOT EXISTS is ignored when comparing (implicit to explicit)",
			from: `
CREATE TABLE t1 (
  t1_1 INT64 NOT NULL,
) PRIMARY KEY(t1_1);
`,
			to: `
CREATE TABLE IF NOT EXISTS t1 (
  t1_1 INT64 NOT NULL,
) PRIMARY KEY(t1_1);
`,
			expected: []string{},
		},
		{
			name: "complex modification produces correct order",
			from: `
CREATE TABLE parent (
  pid INT64 NOT NULL,
) PRIMARY KEY(pid);
CREATE TABLE t1 (
  t1_1 INT64 NOT NULL,
  t1_2 INT64,
  CONSTRAINT FK_t1 FOREIGN KEY (t1_2) REFERENCES parent (pid),
) PRIMARY KEY(t1_1);
CREATE INDEX idx_t1 ON t1(t1_2);
CREATE TABLE t2 (
  t2_1 INT64 NOT NULL,
  t2_2 INT64,
  CONSTRAINT FK_t2 FOREIGN KEY (t2_2) REFERENCES parent (pid),
) PRIMARY KEY(t2_1);
CREATE INDEX idx_t2 ON t2(t2_2);
`,
			to: `
CREATE TABLE parent (
  pid INT64 NOT NULL,
) PRIMARY KEY(pid);
CREATE TABLE t1 (
  t1_1 INT64 NOT NULL,
  t1_2 INT64,
  t1_3 INT64,
  CONSTRAINT FK_t1 FOREIGN KEY (t1_2) REFERENCES parent (pid),
) PRIMARY KEY(t1_1);
CREATE INDEX idx_t1 ON t1(t1_3);
CREATE TABLE t3 (
  t3_1 INT64 NOT NULL,
  t3_2 INT64,
  CONSTRAINT FK_t3 FOREIGN KEY (t3_2) REFERENCES parent (pid),
) PRIMARY KEY(t3_1);
`,
			expected: []string{
				"DROP INDEX idx_t1",
				"ALTER TABLE t1 ADD COLUMN t1_3 INT64",
				"CREATE INDEX idx_t1 ON t1(t1_3)",
				`
CREATE TABLE t3 (
  t3_1 INT64 NOT NULL,
  t3_2 INT64,
  CONSTRAINT FK_t3 FOREIGN KEY (t3_2) REFERENCES parent (pid)
) PRIMARY KEY (t3_1)`,
				"DROP INDEX idx_t2",
				"DROP TABLE t2",
			},
		},
	})
}

func TestDiffColumn(t *testing.T) {
	runDiffTests(t, []diffTestCase{
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
			name: "change column (array element size)",
			from: `
CREATE TABLE t1 (
  t1_1 INT64 NOT NULL,
  t1_2 ARRAY<STRING(10)>,
) PRIMARY KEY(t1_1);
`,
			to: `
CREATE TABLE t1 (
  t1_1 INT64 NOT NULL,
  t1_2 ARRAY<STRING(20)>,
) PRIMARY KEY(t1_1);
`,
			expected: []string{
				`ALTER TABLE t1 ALTER COLUMN t1_2 ARRAY<STRING(20)>`,
			},
		},
		{
			name: "change column (array element type)",
			from: `
CREATE TABLE t1 (
  t1_1 INT64 NOT NULL,
  t1_2 ARRAY<STRING(10)>,
) PRIMARY KEY(t1_1);
`,
			to: `
CREATE TABLE t1 (
  t1_1 INT64 NOT NULL,
  t1_2 ARRAY<INT64>,
) PRIMARY KEY(t1_1);
`,
			expected: []string{
				`ALTER TABLE t1 DROP COLUMN t1_2`,
				`ALTER TABLE t1 ADD COLUMN t1_2 ARRAY<INT64>`,
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
			name: "drop NOT NULL on FK column drops and re-adds the constraint",
			from: `
CREATE TABLE Bars (
  ID INT64 NOT NULL,
) PRIMARY KEY(ID);
CREATE TABLE Foos (
  FooID INT64 NOT NULL,
  BarID INT64 NOT NULL,
  CONSTRAINT FK_FoosBar FOREIGN KEY (BarID) REFERENCES Bars (ID),
) PRIMARY KEY(FooID);
`,
			to: `
CREATE TABLE Bars (
  ID INT64 NOT NULL,
) PRIMARY KEY(ID);
CREATE TABLE Foos (
  FooID INT64 NOT NULL,
  BarID INT64,
  CONSTRAINT FK_FoosBar FOREIGN KEY (BarID) REFERENCES Bars (ID),
) PRIMARY KEY(FooID);
`,
			expected: []string{
				`ALTER TABLE Foos DROP CONSTRAINT FK_FoosBar`,
				`ALTER TABLE Foos ALTER COLUMN BarID INT64`,
				`ALTER TABLE Foos ADD CONSTRAINT FK_FoosBar FOREIGN KEY (BarID) REFERENCES Bars (ID)`,
			},
		},
		{
			name: "add NOT NULL on FK column drops and re-adds the constraint",
			from: `
CREATE TABLE Bars (
  ID INT64 NOT NULL,
) PRIMARY KEY(ID);
CREATE TABLE Foos (
  FooID INT64 NOT NULL,
  BarID INT64,
  CONSTRAINT FK_FoosBar FOREIGN KEY (BarID) REFERENCES Bars (ID),
) PRIMARY KEY(FooID);
`,
			to: `
CREATE TABLE Bars (
  ID INT64 NOT NULL,
) PRIMARY KEY(ID);
CREATE TABLE Foos (
  FooID INT64 NOT NULL,
  BarID INT64 NOT NULL,
  CONSTRAINT FK_FoosBar FOREIGN KEY (BarID) REFERENCES Bars (ID),
) PRIMARY KEY(FooID);
`,
			expected: []string{
				`ALTER TABLE Foos DROP CONSTRAINT FK_FoosBar`,
				`UPDATE Foos SET BarID = 0 WHERE BarID IS NULL`,
				`ALTER TABLE Foos ALTER COLUMN BarID INT64 NOT NULL`,
				`ALTER TABLE Foos ADD CONSTRAINT FK_FoosBar FOREIGN KEY (BarID) REFERENCES Bars (ID)`,
			},
		},
		{
			name: "drop NOT NULL on a column referenced by FK from another table",
			from: `
CREATE TABLE Foos (
  FooID INT64 NOT NULL,
  RefKey INT64 NOT NULL,
) PRIMARY KEY(FooID);
CREATE TABLE Bazs (
  BazID INT64 NOT NULL,
  RefKey INT64 NOT NULL,
  CONSTRAINT FK_BazsFoos FOREIGN KEY (RefKey) REFERENCES Foos (RefKey),
) PRIMARY KEY(BazID);
`,
			to: `
CREATE TABLE Foos (
  FooID INT64 NOT NULL,
  RefKey INT64,
) PRIMARY KEY(FooID);
CREATE TABLE Bazs (
  BazID INT64 NOT NULL,
  RefKey INT64 NOT NULL,
  CONSTRAINT FK_BazsFoos FOREIGN KEY (RefKey) REFERENCES Foos (RefKey),
) PRIMARY KEY(BazID);
`,
			expected: []string{
				`ALTER TABLE Bazs DROP CONSTRAINT FK_BazsFoos`,
				`ALTER TABLE Foos ALTER COLUMN RefKey INT64`,
				`ALTER TABLE Bazs ADD CONSTRAINT FK_BazsFoos FOREIGN KEY (RefKey) REFERENCES Foos (RefKey)`,
			},
		},
		{
			name: "drop NOT NULL when FK on another table is declared before the column owner",
			from: `
CREATE TABLE Foos (
  FooID INT64 NOT NULL,
  RefKey INT64 NOT NULL,
) PRIMARY KEY(FooID);
CREATE TABLE Bazs (
  BazID INT64 NOT NULL,
  RefKey INT64 NOT NULL,
  CONSTRAINT FK_BazsFoos FOREIGN KEY (RefKey) REFERENCES Foos (RefKey),
) PRIMARY KEY(BazID);
`,
			to: `
CREATE TABLE Bazs (
  BazID INT64 NOT NULL,
  RefKey INT64 NOT NULL,
  CONSTRAINT FK_BazsFoos FOREIGN KEY (RefKey) REFERENCES Foos (RefKey),
) PRIMARY KEY(BazID);
CREATE TABLE Foos (
  FooID INT64 NOT NULL,
  RefKey INT64,
) PRIMARY KEY(FooID);
`,
			expected: []string{
				`ALTER TABLE Bazs DROP CONSTRAINT FK_BazsFoos`,
				`ALTER TABLE Foos ALTER COLUMN RefKey INT64`,
				`ALTER TABLE Bazs ADD CONSTRAINT FK_BazsFoos FOREIGN KEY (RefKey) REFERENCES Foos (RefKey)`,
			},
		},
		{
			name: "FK column unchanged is no-op",
			from: `
CREATE TABLE Bars (
  ID INT64 NOT NULL,
) PRIMARY KEY(ID);
CREATE TABLE Foos (
  FooID INT64 NOT NULL,
  BarID INT64 NOT NULL,
  CONSTRAINT FK_FoosBar FOREIGN KEY (BarID) REFERENCES Bars (ID),
) PRIMARY KEY(FooID);
`,
			to: `
CREATE TABLE Bars (
  ID INT64 NOT NULL,
) PRIMARY KEY(ID);
CREATE TABLE Foos (
  FooID INT64 NOT NULL,
  BarID INT64 NOT NULL,
  CONSTRAINT FK_FoosBar FOREIGN KEY (BarID) REFERENCES Bars (ID),
) PRIMARY KEY(FooID);
`,
			expected: []string{},
		},
		{
			name: "DEFAULT change on FK column does not drop the constraint",
			from: `
CREATE TABLE Bars (
  ID INT64 NOT NULL,
) PRIMARY KEY(ID);
CREATE TABLE Foos (
  FooID INT64 NOT NULL,
  BarID INT64 NOT NULL DEFAULT (1),
  CONSTRAINT FK_FoosBar FOREIGN KEY (BarID) REFERENCES Bars (ID),
) PRIMARY KEY(FooID);
`,
			to: `
CREATE TABLE Bars (
  ID INT64 NOT NULL,
) PRIMARY KEY(ID);
CREATE TABLE Foos (
  FooID INT64 NOT NULL,
  BarID INT64 NOT NULL DEFAULT (2),
  CONSTRAINT FK_FoosBar FOREIGN KEY (BarID) REFERENCES Bars (ID),
) PRIMARY KEY(FooID);
`,
			expected: []string{
				`ALTER TABLE Foos ALTER COLUMN BarID INT64 NOT NULL DEFAULT (2)`,
			},
		},
		{
			name: "NOT NULL change on a column without FK does not emit FK ops",
			from: `
CREATE TABLE t1 (
  t1_1 INT64 NOT NULL,
  t1_2 INT64 NOT NULL,
) PRIMARY KEY(t1_1);
`,
			to: `
CREATE TABLE t1 (
  t1_1 INT64 NOT NULL,
  t1_2 INT64,
) PRIMARY KEY(t1_1);
`,
			expected: []string{
				`ALTER TABLE t1 ALTER COLUMN t1_2 INT64`,
			},
		},
		{
			name: "change column (timestamp, drop NOT NULL)",
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
			name: "change column (timestamp, add allow_commit_timestamp)",
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
			name: "change column (timestamp, add NOT NULL and allow_commit_timestamp)",
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
			name: "change column (timestamp, drop NOT NULL and allow_commit_timestamp)",
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
			name: "add column FLOAT32 not null",
			from: `
CREATE TABLE t1 (
  t1_1 INT64 NOT NULL,
) PRIMARY KEY(t1_1);
`,
			to: `
CREATE TABLE t1 (
  t1_1 INT64 NOT NULL,
  t1_2 FLOAT32 NOT NULL,
) PRIMARY KEY(t1_1);
`,
			expected: []string{
				`ALTER TABLE t1 ADD COLUMN t1_2 FLOAT32 NOT NULL DEFAULT (0)`,
				`ALTER TABLE t1 ALTER COLUMN t1_2 DROP DEFAULT`,
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
			name: "generated column with JSON_QUERY identity is no-op",
			from: `
CREATE TABLE t1 (
  id STRING(40) NOT NULL,
  data JSON,
  has_value BOOL AS (IF(JSON_QUERY(data, '$.value') IS NOT NULL, true, false)) STORED,
) PRIMARY KEY(id);
`,
			to: `
CREATE TABLE t1 (
  id STRING(40) NOT NULL,
  data JSON,
  has_value BOOL AS (IF(JSON_QUERY(data, '$.value') IS NOT NULL, true, false)) STORED,
) PRIMARY KEY(id);
`,
			expected: []string{},
		},
		{
			name: "generated column with JSON_QUERY path change is detected",
			from: `
CREATE TABLE t1 (
  id STRING(40) NOT NULL,
  data JSON,
  has_value BOOL AS (IF(JSON_QUERY(data, '$.value') IS NOT NULL, true, false)) STORED,
) PRIMARY KEY(id);
`,
			to: `
CREATE TABLE t1 (
  id STRING(40) NOT NULL,
  data JSON,
  has_value BOOL AS (IF(JSON_QUERY(data, '$.other') IS NOT NULL, true, false)) STORED,
) PRIMARY KEY(id);
`,
			expected: []string{
				`ALTER TABLE t1 DROP COLUMN has_value`,
				`ALTER TABLE t1 ADD COLUMN has_value BOOL AS (IF(JSON_QUERY(data, "$.other") IS NOT NULL, TRUE, FALSE)) STORED`,
			},
		},
		{
			name: "identity column empty parameter list is semantically equivalent",
			from: `
CREATE TABLE t1 (
  t1_1 INT64 GENERATED BY DEFAULT AS IDENTITY,
) PRIMARY KEY(t1_1);
`,
			to: `
CREATE TABLE t1 (
  t1_1 INT64 GENERATED BY DEFAULT AS IDENTITY(),
) PRIMARY KEY(t1_1);
`,
			expected: []string{},
		},
		{
			name: "default value identity is detected as no diff",
			from: `
CREATE TABLE t1 (
  t1_1 INT64 NOT NULL,
  created_at TIMESTAMP DEFAULT (CURRENT_TIMESTAMP()),
) PRIMARY KEY(t1_1);
`,
			to: `
CREATE TABLE t1 (
  t1_1 INT64 NOT NULL,
  created_at TIMESTAMP DEFAULT (CURRENT_TIMESTAMP()),
) PRIMARY KEY(t1_1);
`,
			expected: []string{},
		},
	})
}

func TestDiffInterleave(t *testing.T) {
	runDiffTests(t, []diffTestCase{
		{
			name: "change parent primary key type recreates interleaved children",
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
				`
CREATE TABLE t1 (
  t1_1 STRING(36) NOT NULL
) PRIMARY KEY (t1_1)`,
				`
CREATE TABLE t2 (
  t1_1 INT64 NOT NULL,
  t2_1 INT64 NOT NULL,
  t2_2 INT64 NOT NULL,
  t2_3 INT64 NOT NULL
) PRIMARY KEY (t1_1, t2_1),
  INTERLEAVE IN PARENT t1 ON DELETE NO ACTION`,
				`CREATE INDEX idx_t2 ON t2(t2_1)`,
				`
CREATE TABLE t3 (
  t1_1 INT64 NOT NULL,
  t3_1 INT64 NOT NULL
) PRIMARY KEY (t1_1, t3_1),
  INTERLEAVE IN PARENT t1 ON DELETE NO ACTION`,
				`CREATE INDEX idx_t3 ON t3(t3_1)`,
			},
		},
		{
			name: "multi-level interleave drops grandchild before grandparent",
			from: `
CREATE TABLE gp (
  gp_id INT64 NOT NULL,
) PRIMARY KEY(gp_id);
CREATE TABLE p (
  gp_id INT64 NOT NULL,
  p_id INT64 NOT NULL,
) PRIMARY KEY(gp_id, p_id), INTERLEAVE IN PARENT gp ON DELETE NO ACTION;
CREATE TABLE c (
  gp_id INT64 NOT NULL,
  p_id INT64 NOT NULL,
  c_id INT64 NOT NULL,
) PRIMARY KEY(gp_id, p_id, c_id), INTERLEAVE IN PARENT p ON DELETE NO ACTION;
`,
			to: `
CREATE TABLE gp (
  gp_id INT64 NOT NULL,
) PRIMARY KEY(gp_id);
`,
			expected: []string{
				"DROP TABLE c",
				"DROP TABLE p",
			},
		},
		{
			name: "multi-level interleave creates grandparent first",
			from: `
CREATE TABLE other (
  id INT64 NOT NULL,
) PRIMARY KEY(id);`,
			to: `
CREATE TABLE other (
  id INT64 NOT NULL,
) PRIMARY KEY(id);
CREATE TABLE gp (
  gp_id INT64 NOT NULL,
) PRIMARY KEY(gp_id);
CREATE TABLE p (
  gp_id INT64 NOT NULL,
  p_id INT64 NOT NULL,
) PRIMARY KEY(gp_id, p_id), INTERLEAVE IN PARENT gp ON DELETE NO ACTION;
CREATE TABLE c (
  gp_id INT64 NOT NULL,
  p_id INT64 NOT NULL,
  c_id INT64 NOT NULL,
) PRIMARY KEY(gp_id, p_id, c_id), INTERLEAVE IN PARENT p ON DELETE NO ACTION;
`,
			expected: []string{
				`
CREATE TABLE gp (
  gp_id INT64 NOT NULL
) PRIMARY KEY (gp_id)`,
				`
CREATE TABLE p (
  gp_id INT64 NOT NULL,
  p_id INT64 NOT NULL
) PRIMARY KEY (gp_id, p_id),
  INTERLEAVE IN PARENT gp ON DELETE NO ACTION`,
				`
CREATE TABLE c (
  gp_id INT64 NOT NULL,
  p_id INT64 NOT NULL,
  c_id INT64 NOT NULL
) PRIMARY KEY (gp_id, p_id, c_id),
  INTERLEAVE IN PARENT p ON DELETE NO ACTION`,
			},
		},
		{
			name: "interleave on delete no action is the default (explicit to implicit)",
			from: `
CREATE TABLE t1 (
  t1_1 INT64 NOT NULL,
) PRIMARY KEY(t1_1);
CREATE TABLE t2 (
  t1_1 INT64 NOT NULL,
  t2_1 INT64 NOT NULL,
) PRIMARY KEY(t1_1, t2_1), INTERLEAVE IN PARENT t1 ON DELETE NO ACTION;
`,
			to: `
CREATE TABLE t1 (
  t1_1 INT64 NOT NULL,
) PRIMARY KEY(t1_1);
CREATE TABLE t2 (
  t1_1 INT64 NOT NULL,
  t2_1 INT64 NOT NULL,
) PRIMARY KEY(t1_1, t2_1), INTERLEAVE IN PARENT t1;
`,
			expected: []string{},
		},
		{
			name: "interleave on delete no action is the default (implicit to explicit)",
			from: `
CREATE TABLE t1 (
  t1_1 INT64 NOT NULL,
) PRIMARY KEY(t1_1);
CREATE TABLE t2 (
  t1_1 INT64 NOT NULL,
  t2_1 INT64 NOT NULL,
) PRIMARY KEY(t1_1, t2_1), INTERLEAVE IN PARENT t1;
`,
			to: `
CREATE TABLE t1 (
  t1_1 INT64 NOT NULL,
) PRIMARY KEY(t1_1);
CREATE TABLE t2 (
  t1_1 INT64 NOT NULL,
  t2_1 INT64 NOT NULL,
) PRIMARY KEY(t1_1, t2_1), INTERLEAVE IN PARENT t1 ON DELETE NO ACTION;
`,
			expected: []string{},
		},
		{
			name: "interleave on delete cascade is detected as a real change",
			from: `
CREATE TABLE t1 (
  t1_1 INT64 NOT NULL,
) PRIMARY KEY(t1_1);
CREATE TABLE t2 (
  t1_1 INT64 NOT NULL,
  t2_1 INT64 NOT NULL,
) PRIMARY KEY(t1_1, t2_1), INTERLEAVE IN PARENT t1;
`,
			to: `
CREATE TABLE t1 (
  t1_1 INT64 NOT NULL,
) PRIMARY KEY(t1_1);
CREATE TABLE t2 (
  t1_1 INT64 NOT NULL,
  t2_1 INT64 NOT NULL,
) PRIMARY KEY(t1_1, t2_1), INTERLEAVE IN PARENT t1 ON DELETE CASCADE;
`,
			expected: []string{
				"DROP TABLE t2",
				`
CREATE TABLE t2 (
  t1_1 INT64 NOT NULL,
  t2_1 INT64 NOT NULL
) PRIMARY KEY (t1_1, t2_1),
  INTERLEAVE IN PARENT t1 ON DELETE CASCADE`,
			},
		},
		{
			name: "interleave parent keyword removal is detected as a real change",
			from: `
CREATE TABLE t1 (
  t1_1 INT64 NOT NULL,
) PRIMARY KEY(t1_1);
CREATE TABLE t2 (
  t1_1 INT64 NOT NULL,
  t2_1 INT64 NOT NULL,
) PRIMARY KEY(t1_1, t2_1), INTERLEAVE IN PARENT t1;
`,
			to: `
CREATE TABLE t1 (
  t1_1 INT64 NOT NULL,
) PRIMARY KEY(t1_1);
CREATE TABLE t2 (
  t1_1 INT64 NOT NULL,
  t2_1 INT64 NOT NULL,
) PRIMARY KEY(t1_1, t2_1), INTERLEAVE IN t1;
`,
			expected: []string{
				"DROP TABLE t2",
				`
CREATE TABLE t2 (
  t1_1 INT64 NOT NULL,
  t2_1 INT64 NOT NULL
) PRIMARY KEY (t1_1, t2_1),
  INTERLEAVE IN t1`,
			},
		},
	})
}

func TestDiffRowDeletionPolicy(t *testing.T) {
	runDiffTests(t, []diffTestCase{
		{
			name: "create table with ROW DELETION POLICY",
			from: ``,
			to: `
CREATE TABLE t1 (
  t1_1 INT64 NOT NULL,
  t1_2 TIMESTAMP NOT NULL,
) PRIMARY KEY(t1_1), ROW DELETION POLICY (OLDER_THAN(t1_2, INTERVAL 30 DAY));
		`,
			expected: []string{
				`
CREATE TABLE t1 (
  t1_1 INT64 NOT NULL,
  t1_2 TIMESTAMP NOT NULL
) PRIMARY KEY (t1_1), ROW DELETION POLICY ( OLDER_THAN ( t1_2, INTERVAL 30 DAY ))`,
			},
		},
		{
			name: "add ROW DELETION POLICY",
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
			name: "replace ROW DELETION POLICY",
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
			name: "drop ROW DELETION POLICY",
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
	})
}
