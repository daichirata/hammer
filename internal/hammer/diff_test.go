package hammer_test

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"testing"

	"github.com/google/go-cmp/cmp"

	"github.com/daichirata/hammer/internal/hammer"
)

func TestDiff(t *testing.T) {
	values := []struct {
		name                string
		from                string
		to                  string
		ignoreAlterDatabase bool
		ignoreProtoBundles  bool
		expected            []string
	}{
		// === TABLE ===
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
				`
CREATE TABLE t2 (
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
			name: "Do not drop unrelated constraint (same column name)",
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
			name: "Do not drop unrelated constraint (same referenced column name)",
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
				`
CREATE TABLE t1 (
  t1_1 INT64,
  t1_2 INT64
) PRIMARY KEY (t1_2)`,
				`ALTER TABLE t2 ADD CONSTRAINT FK_t2 FOREIGN KEY (t2_1) REFERENCES t1 (t1_1)`,
			},
		},
		{
			name: "Do not recreate constraint for default delete action.",
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
			name: "Create table with ROW DELETION POLICY",
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
		// === INDEX ===
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
		// === SEARCH INDEX ===
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
		// === VIEW ===
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
		// === ROLE ===
		{
			name: "create role",
			from: `
			CREATE ROLE role1;
			`,
			to: `
			CREATE ROLE role1;
			CREATE ROLE role2;
			`,
			expected: []string{
				`CREATE ROLE role2`,
			},
		},
		{
			name: "drop role",
			from: `
			CREATE ROLE role1;
			CREATE ROLE role2;
			`,
			to: `
			CREATE ROLE role1;
`,
			expected: []string{
				`DROP ROLE role2`,
			},
		},
		{
			name: "drop unused role only",
			from: `
			CREATE ROLE role1;
			CREATE ROLE role2;
			GRANT SELECT ON TABLE T1 TO ROLE role1;
			`,
			to: `
			CREATE ROLE role1;
			GRANT SELECT ON TABLE T1 TO ROLE role1;
			`,
			expected: []string{
				`DROP ROLE role2`,
			},
		},
		// === SCHEMA ===
		{
			name: "create schema",
			from: ``,
			to: `
			CREATE SCHEMA sch1;
			`,
			expected: []string{
				`CREATE SCHEMA sch1`,
			},
		},
		{
			name: "drop schema",
			from: `
			CREATE SCHEMA sch1;
			CREATE SCHEMA sch2;
			`,
			to: `
			CREATE SCHEMA sch1;
			`,
			expected: []string{
				`DROP SCHEMA sch2`,
			},
		},
		{
			name: "schema unchanged is a no-op",
			from: `
			CREATE SCHEMA sch1;
			`,
			to: `
			CREATE SCHEMA sch1;
			`,
			expected: nil,
		},
		{
			name: "create schema before table in that schema",
			from: ``,
			to: `
			CREATE SCHEMA sch1;
			CREATE TABLE sch1.t1 (
			  t1_1 INT64 NOT NULL,
			) PRIMARY KEY(t1_1);
			`,
			expected: []string{
				`CREATE SCHEMA sch1`,
				`
CREATE TABLE sch1.t1 (
  t1_1 INT64 NOT NULL
) PRIMARY KEY (t1_1)`,
			},
		},
		{
			name: "drop schema after table in that schema",
			from: `
			CREATE SCHEMA sch1;
			CREATE TABLE sch1.t1 (
			  t1_1 INT64 NOT NULL,
			) PRIMARY KEY(t1_1);
			`,
			to: ``,
			expected: []string{
				`DROP TABLE sch1.t1`,
				`DROP SCHEMA sch1`,
			},
		},
		{
			name: "create schema before schema-qualified table, index and view",
			from: ``,
			to: `
			CREATE SCHEMA sch1;
			CREATE TABLE sch1.t1 (
			  id INT64 NOT NULL,
			  name STRING(MAX),
			) PRIMARY KEY(id);
			CREATE INDEX idx1 ON sch1.t1(name);
			CREATE VIEW sch1.v1 SQL SECURITY INVOKER AS SELECT id FROM sch1.t1;
			`,
			expected: []string{
				`CREATE SCHEMA sch1`,
				`
CREATE TABLE sch1.t1 (
  id INT64 NOT NULL,
  name STRING(MAX)
) PRIMARY KEY (id)`,
				`CREATE INDEX idx1 ON sch1.t1(name)`,
				`CREATE VIEW sch1.v1 SQL SECURITY INVOKER AS SELECT id FROM sch1.t1`,
			},
		},
		{
			name: "drop schema after its qualified table, index and view",
			from: `
			CREATE SCHEMA sch1;
			CREATE TABLE sch1.t1 (
			  id INT64 NOT NULL,
			  name STRING(MAX),
			) PRIMARY KEY(id);
			CREATE INDEX idx1 ON sch1.t1(name);
			CREATE VIEW sch1.v1 SQL SECURITY INVOKER AS SELECT id FROM sch1.t1;
			`,
			to: ``,
			expected: []string{
				`DROP INDEX idx1`,
				`DROP TABLE sch1.t1`,
				`DROP VIEW sch1.v1`,
				`DROP SCHEMA sch1`,
			},
		},
		// === GRANT / REVOKE ===
		{
			name: "grant role",
			from: `
			GRANT SELECT ON TABLE T1 TO ROLE role1;
			`,
			to: `
			GRANT SELECT ON TABLE T1 TO ROLE role1;
			GRANT SELECT ON TABLE T2 TO ROLE role2;
			`,
			expected: []string{
				`GRANT SELECT ON TABLE T2 TO ROLE role2`,
			},
		},
		{
			name: "grant select on view",
			from: `
				CREATE ROLE role1;
			`,
			to: `
				CREATE ROLE role1;
				GRANT SELECT ON VIEW V1 TO ROLE role1;
			`,
			expected: []string{
				`GRANT SELECT ON VIEW V1 TO ROLE role1`,
			},
		},
		{
			name: "grant select on change stream",
			from: `
				CREATE ROLE role1;
			`,
			to: `
				CREATE ROLE role1;
				GRANT SELECT ON CHANGE STREAM cs1 TO ROLE role1;
			`,
			expected: []string{
				`GRANT SELECT ON CHANGE STREAM cs1 TO ROLE role1`,
			},
		},
		{
			name: "grant execute on table function",
			from: `
				CREATE ROLE role1;
			`,
			to: `
				CREATE ROLE role1;
				GRANT EXECUTE ON TABLE FUNCTION tf1 TO ROLE role1;
			`,
			expected: []string{
				`GRANT EXECUTE ON TABLE FUNCTION tf1 TO ROLE role1`,
			},
		},
		{
			name: "grant role with same roles in different order",
			from: `
			GRANT SELECT ON TABLE T1 TO ROLE role1, role2;
			`,
			to: `
			GRANT SELECT ON TABLE T1 TO ROLE role2, role1;
			`,
			expected: []string{
				`REVOKE SELECT ON TABLE T1 FROM ROLE role1, role2`,
				`GRANT SELECT ON TABLE T1 TO ROLE role2, role1`,
			},
		},
		{
			name: "revoke role",
			from: `
			GRANT SELECT ON TABLE T1 TO ROLE role1;
			GRANT SELECT ON TABLE T2 TO ROLE role2;
			`,
			to: `
			GRANT SELECT ON TABLE T1 TO ROLE role1;
			`,
			expected: []string{
				`REVOKE SELECT ON TABLE T2 FROM ROLE role2`,
			},
		},
		{
			name: "replace grant role",
			from: `
			GRANT SELECT ON TABLE T1 TO ROLE role1;
			`,
			to: `
			GRANT SELECT, INSERT ON TABLE T1, T2 TO ROLE role1, role2;
			`,
			expected: []string{
				`REVOKE SELECT ON TABLE T1 FROM ROLE role1`,
				`GRANT SELECT, INSERT ON TABLE T1, T2 TO ROLE role1, role2`,
			},
		},
		{
			name: "revoke only, keep role",
			from: `
			CREATE ROLE role1;
			GRANT SELECT ON TABLE T1 TO ROLE role1;
			`,
			to: `
			CREATE ROLE role1;
			`,
			expected: []string{
				`REVOKE SELECT ON TABLE T1 FROM ROLE role1`,
			},
		},
		{
			name: "grant same role on different tables",
			from: `
			CREATE ROLE role1;
			GRANT SELECT ON TABLE T1 TO ROLE role1;
			`,
			to: `
			CREATE ROLE role1;
			GRANT SELECT ON TABLE T1 TO ROLE role1;
			GRANT SELECT ON TABLE T2 TO ROLE role1;
			`,
			expected: []string{
				`GRANT SELECT ON TABLE T2 TO ROLE role1`,
			},
		},
		{
			name: "revoke same role on different tables",
			from: `
			CREATE ROLE role1;
			GRANT SELECT ON TABLE T1 TO ROLE role1;
			GRANT SELECT ON TABLE T2 TO ROLE role1;
			`,
			to: `
			CREATE ROLE role1;
			GRANT SELECT ON TABLE T1 TO ROLE role1;
			`,
			expected: []string{
				`REVOKE SELECT ON TABLE T2 FROM ROLE role1`,
			},
		},
		{
			name: "grant on multiple tables in different order",
			from: `
				GRANT SELECT ON TABLE T1, T2 TO ROLE role1;
			`,
			to: `
				GRANT SELECT ON TABLE T2, T1 TO ROLE role1;
			`,
			expected: []string{
				`REVOKE SELECT ON TABLE T1, T2 FROM ROLE role1`,
				`GRANT SELECT ON TABLE T2, T1 TO ROLE role1`,
			},
		},
		{
			name: "replace privilege type on same table",
			from: `
				GRANT SELECT ON TABLE T1 TO ROLE role1;
			`,
			to: `
				GRANT INSERT ON TABLE T1 TO ROLE role1;
			`,
			expected: []string{
				`REVOKE SELECT ON TABLE T1 FROM ROLE role1`,
				`GRANT INSERT ON TABLE T1 TO ROLE role1`,
			},
		},
		{
			name: "grant multiple privileges at once",
			from: ``,
			to: `
				GRANT SELECT, INSERT, DELETE ON TABLE T1 TO ROLE role1;
			`,
			expected: []string{
				`GRANT SELECT, INSERT, DELETE ON TABLE T1 TO ROLE role1`,
			},
		},
		{
			name: "revoke select column and grant new columns",
			from: `
				GRANT SELECT(col1) ON TABLE T1 TO ROLE role1;
			`,
			to: `
				GRANT SELECT(col1, col2) ON TABLE T1 TO ROLE role1;
			`,
			expected: []string{
				`REVOKE SELECT(col1) ON TABLE T1 FROM ROLE role1`,
				`GRANT SELECT(col1, col2) ON TABLE T1 TO ROLE role1`,
			},
		},
		{
			name: "grant insert with column list",
			from: `
CREATE TABLE t1 (
  id INT64 NOT NULL,
  a INT64,
  b INT64,
) PRIMARY KEY(id);
CREATE ROLE r1;
`,
			to: `
CREATE TABLE t1 (
  id INT64 NOT NULL,
  a INT64,
  b INT64,
) PRIMARY KEY(id);
CREATE ROLE r1;
GRANT INSERT(a, b) ON TABLE t1 TO ROLE r1;
`,
			expected: []string{
				"GRANT INSERT(a, b) ON TABLE t1 TO ROLE r1",
			},
		},
		{
			name: "grant update with column list",
			from: `
CREATE TABLE t1 (
  id INT64 NOT NULL,
  a INT64,
  b INT64,
) PRIMARY KEY(id);
CREATE ROLE r1;
`,
			to: `
CREATE TABLE t1 (
  id INT64 NOT NULL,
  a INT64,
  b INT64,
) PRIMARY KEY(id);
CREATE ROLE r1;
GRANT UPDATE(a) ON TABLE t1 TO ROLE r1;
`,
			expected: []string{
				"GRANT UPDATE(a) ON TABLE t1 TO ROLE r1",
			},
		},
		{
			name: "grant delete privilege",
			from: `
CREATE TABLE t1 (
  id INT64 NOT NULL,
) PRIMARY KEY(id);
CREATE ROLE r1;
`,
			to: `
CREATE TABLE t1 (
  id INT64 NOT NULL,
) PRIMARY KEY(id);
CREATE ROLE r1;
GRANT DELETE ON TABLE t1 TO ROLE r1;
`,
			expected: []string{
				"GRANT DELETE ON TABLE t1 TO ROLE r1",
			},
		},
		{
			name: "revoke on table before DROP TABLE",
			from: `
				CREATE ROLE role1;
				CREATE TABLE T1 (id INT64);
				GRANT SELECT ON TABLE T1 TO ROLE role1;
			`,
			to: `
				CREATE ROLE role1;
			`,
			expected: []string{
				`DROP TABLE T1`,
			},
		},
		{
			name: "revoke on table before DROP TABLE (PK change triggers drop)",
			from: `
				CREATE ROLE role1;
				CREATE TABLE T1 (
  id INT64,
  name STRING(100),
) PRIMARY KEY(id);
				GRANT SELECT ON TABLE T1 TO ROLE role1;
			`,
			to: `
				CREATE ROLE role1;
				CREATE TABLE T1 (
  id INT64,
  name STRING(100),
) PRIMARY KEY(name);
				GRANT SELECT ON TABLE T1 TO ROLE role1;
			`,
			expected: []string{
				`DROP TABLE T1`,
				`
CREATE TABLE T1 (
  id INT64,
  name STRING(100)
) PRIMARY KEY (name)`,
				`GRANT SELECT ON TABLE T1 TO ROLE role1`,
			},
		},
		{
			name: "revoke on table before DROP TABLE (interleave parent changed)",
			from: `
				CREATE ROLE role1;
				CREATE TABLE P1 (
  pid INT64 NOT NULL,
) PRIMARY KEY(pid);
				CREATE TABLE P2 (
  pid INT64 NOT NULL,
) PRIMARY KEY(pid);
				CREATE TABLE T1 (
  id INT64 NOT NULL,
) PRIMARY KEY(id), INTERLEAVE IN PARENT P1;
				GRANT SELECT ON TABLE T1 TO ROLE role1;
			`,
			to: `
				CREATE ROLE role1;
				CREATE TABLE P1 (
  pid INT64 NOT NULL,
) PRIMARY KEY(pid);
				CREATE TABLE P2 (
  pid INT64 NOT NULL,
) PRIMARY KEY(pid);
				CREATE TABLE T1 (
  id INT64 NOT NULL,
) PRIMARY KEY(id), INTERLEAVE IN PARENT P2;
				GRANT SELECT ON TABLE T1 TO ROLE role1;
			`,
			expected: []string{
				`DROP TABLE T1`,
				`
CREATE TABLE T1 (
  id INT64 NOT NULL
) PRIMARY KEY (id),
  INTERLEAVE IN PARENT P2`,
				`GRANT SELECT ON TABLE T1 TO ROLE role1`,
			},
		},
		{
			name: "revoke on table before DROP TABLE (removing interleave)",
			from: `
				CREATE ROLE role1;
				CREATE TABLE P1 (
  pid INT64 NOT NULL,
) PRIMARY KEY(pid);
				CREATE TABLE T1 (
  id INT64 NOT NULL,
) PRIMARY KEY(id), INTERLEAVE IN PARENT P1;
				GRANT SELECT ON TABLE T1 TO ROLE role1;
			`,
			to: `
				CREATE ROLE role1;
				CREATE TABLE P1 (
  pid INT64 NOT NULL,
) PRIMARY KEY(pid);
				CREATE TABLE T1 (
  id INT64 NOT NULL,
) PRIMARY KEY(id);
				GRANT SELECT ON TABLE T1 TO ROLE role1;
			`,
			expected: []string{
				`DROP TABLE T1`,
				`
CREATE TABLE T1 (
  id INT64 NOT NULL
) PRIMARY KEY (id)`,
				`GRANT SELECT ON TABLE T1 TO ROLE role1`,
			},
		},
		{
			name: "revoke on column before DROP COLUMN",
			from: `
				CREATE ROLE role1;
				CREATE TABLE T1 (id INT64, name STRING(100));
				GRANT SELECT(name) ON TABLE T1 TO ROLE role1;
			`,
			to: `
				CREATE ROLE role1;
				CREATE TABLE T1 (id INT64);
			`,
			expected: []string{
				`ALTER TABLE T1 DROP COLUMN name`,
			},
		},
		{
			name: "revoke on view before DROP VIEW",
			from: `
				CREATE ROLE role1;
				CREATE VIEW V1 SQL SECURITY INVOKER AS SELECT 1;
				GRANT SELECT ON VIEW V1 TO ROLE role1;
			`,
			to: `
				CREATE ROLE role1;
			`,
			expected: []string{
				`DROP VIEW V1`,
			},
		},
		{
			name: "revoke on change stream before DROP CHANGE STREAM",
			from: `
				CREATE ROLE role1;
				CREATE CHANGE STREAM CS1 FOR ALL;
				GRANT SELECT ON CHANGE STREAM CS1 TO ROLE role1;
			`,
			to: `
				CREATE ROLE role1;
			`,
			expected: []string{
				`DROP CHANGE STREAM CS1`,
			},
		},
		{
			name: "revoke table grant before DROP ROLE",
			from: `
				CREATE ROLE role1;
				CREATE TABLE T1 (id INT64);
				GRANT SELECT ON TABLE T1 TO ROLE role1;
			`,
			to: `
				CREATE TABLE T1 (id INT64);
			`,
			expected: []string{
				`REVOKE SELECT ON TABLE T1 FROM ROLE role1`,
				`DROP ROLE role1`,
			},
		},
		{
			name: "revoke view grant before DROP ROLE",
			from: `
				CREATE ROLE role1;
				CREATE VIEW V1 SQL SECURITY INVOKER AS SELECT 1;
				GRANT SELECT ON VIEW V1 TO ROLE role1;
			`,
			to: `
				CREATE VIEW V1 SQL SECURITY INVOKER AS SELECT 1;
			`,
			expected: []string{
				`CREATE OR REPLACE VIEW V1 SQL SECURITY INVOKER AS SELECT 1`,
				`REVOKE SELECT ON VIEW V1 FROM ROLE role1`,
				`DROP ROLE role1`,
			},
		},
		{
			name: "revoke change stream grant before DROP ROLE",
			from: `
				CREATE ROLE role1;
				CREATE CHANGE STREAM CS1 FOR ALL;
				GRANT SELECT ON CHANGE STREAM CS1 TO ROLE role1;
			`,
			to: `
				CREATE CHANGE STREAM CS1 FOR ALL;
			`,
			expected: []string{
				`REVOKE SELECT ON CHANGE STREAM CS1 FROM ROLE role1`,
				`DROP ROLE role1`,
			},
		},
		{
			name: "drop role: revoke only for resources that remain in target",
			from: `
				CREATE ROLE role1;
				CREATE TABLE T1 (id INT64);
				CREATE TABLE T2 (id INT64);
				GRANT SELECT ON TABLE T1 TO ROLE role1;
				GRANT SELECT ON TABLE T2 TO ROLE role1;
			`,
			to: `
				CREATE TABLE T1 (id INT64);
			`,
			expected: []string{
				`DROP TABLE T2`,
				`REVOKE SELECT ON TABLE T1 FROM ROLE role1`,
				`DROP ROLE role1`,
			},
		},
		// === CHANGE STREAM ===
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
		// === ALTER DATABASE ===
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
		// === PROTO BUNDLE ===
		{
			name: "ignore create proto bundle in from",
			from: `
CREATE TABLE t1 (
  t1_1 INT64 NOT NULL,
) PRIMARY KEY(t1_1);
CREATE PROTO BUNDLE (foo.bar);
`,
			to: `
CREATE TABLE t1 (
  t1_1 INT64 NOT NULL,
) PRIMARY KEY(t1_1);
`,
			ignoreProtoBundles: true,
			expected:           []string{},
		},
		{
			name: "ignore create proto bundle in to",
			from: `
CREATE TABLE t1 (
  t1_1 INT64 NOT NULL,
) PRIMARY KEY(t1_1);
`,
			to: `
CREATE TABLE t1 (
  t1_1 INT64 NOT NULL,
) PRIMARY KEY(t1_1);
CREATE PROTO BUNDLE (foo.bar);
`,
			ignoreProtoBundles: true,
			expected:           []string{},
		},
		{
			name: "ignore create proto bundle in both",
			from: `
CREATE TABLE t1 (
  t1_1 INT64 NOT NULL,
) PRIMARY KEY(t1_1);
CREATE PROTO BUNDLE (foo.bar);
`,
			to: `
CREATE TABLE t1 (
  t1_1 INT64 NOT NULL,
) PRIMARY KEY(t1_1);
CREATE PROTO BUNDLE (foo.bar, baz.qux);
`,
			ignoreProtoBundles: true,
			expected:           []string{},
		},
		// === IDENTIFIERS / NAMING ===
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

			d1, err := StringSource(v.from).DDL(ctx, &hammer.DDLOption{IgnoreAlterDatabase: v.ignoreAlterDatabase, IgnoreProtoBundles: v.ignoreProtoBundles})
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			d2, err := StringSource(v.to).DDL(ctx, &hammer.DDLOption{IgnoreAlterDatabase: v.ignoreAlterDatabase, IgnoreProtoBundles: v.ignoreProtoBundles})
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}

			ddl, err := hammer.Diff(d1, d2)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			actual := convertStrings(ddl)
			want := make([]string, len(v.expected))
			for i, s := range v.expected {
				want[i] = strings.TrimPrefix(s, "\n")
			}
			if !slices.Equal(want, actual) {
				t.Error(formatDDLMismatch(want, actual))
			}
		})
	}
}

type StringSource string

func (s StringSource) String() string { return string(s) }
func (s StringSource) DDL(_ context.Context, o *hammer.DDLOption) (hammer.DDL, error) {
	return hammer.ParseDDL("string", s.String(), o)
}

func convertStrings(ddl hammer.DDL) []string {
	ret := make([]string, len(ddl.List))
	for i, stmt := range ddl.List {
		ret[i] = stmt.SQL()
	}
	return ret
}

func formatDDLMismatch(expected, actual []string) string {
	var b, diffs strings.Builder
	for _, blk := range []struct {
		label string
		stmts []string
	}{{"want", expected}, {"got", actual}} {
		fmt.Fprintf(&b, "\n--- %s (%d statement(s)) ---\n", blk.label, len(blk.stmts))
		for i, s := range blk.stmts {
			fmt.Fprintf(&b, "[%d] %s\n", i, strings.ReplaceAll(s, "\n", "\n    "))
		}
	}
	for i := range min(len(expected), len(actual)) {
		if d := cmp.Diff(strings.Split(expected[i], "\n"), strings.Split(actual[i], "\n")); d != "" {
			fmt.Fprintf(&diffs, "[%d]\n%s", i, d)
		}
	}
	if diffs.Len() > 0 {
		fmt.Fprintf(&b, "\n--- differing statements (line-by-line, -want +got) ---\n%s", &diffs)
	}
	return b.String()
}
