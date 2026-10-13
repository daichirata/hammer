package hammer_test

import (
	"context"
	"strings"
	"testing"

	"github.com/daichirata/hammer/internal/hammer"
)

func TestNewSourceStdin(t *testing.T) {
	source, err := hammer.NewSource(context.Background(), "-")
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := source.(*hammer.ReaderSource); !ok {
		t.Errorf("got: %T, want: *hammer.ReaderSource", source)
	}
	if got := source.String(); got != "-" {
		t.Errorf("got: %v, want: -", got)
	}
}

func TestReaderSourceDDL(t *testing.T) {
	schema := `CREATE TABLE t1 (
  t1_1 INT64 NOT NULL,
) PRIMARY KEY(t1_1);`

	source := hammer.NewReaderSource("-", strings.NewReader(schema))
	ddl, err := source.DDL(context.Background(), &hammer.DDLOption{})
	if err != nil {
		t.Fatal(err)
	}
	if len(ddl.List) != 1 {
		t.Fatalf("got: %d statements, want: 1", len(ddl.List))
	}
	want := "CREATE TABLE t1 (\n  t1_1 INT64 NOT NULL\n) PRIMARY KEY (t1_1)"
	if got := ddl.List[0].SQL(); got != want {
		t.Errorf("got: %q, want: %q", got, want)
	}
}

func TestReaderSourceDDLEmpty(t *testing.T) {
	for _, schema := range []string{"", " \n\t"} {
		source := hammer.NewReaderSource("-", strings.NewReader(schema))
		if _, err := source.DDL(context.Background(), &hammer.DDLOption{}); err == nil {
			t.Errorf("expected error for schema %q", schema)
		}
	}
}
