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

type diffTestCase struct {
	name                string
	from                string
	to                  string
	ignoreAlterDatabase bool
	ignoreProtoBundles  bool
	expected            []string
	wantErr             string
}

// runDiffTests checks, besides the from -> to output, that each side diffs to nothing against itself
// and that the reverse diff (to -> from) can also be generated.
func runDiffTests(t *testing.T, tests []diffTestCase) {
	t.Helper()
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			option := &hammer.DDLOption{IgnoreAlterDatabase: tt.ignoreAlterDatabase, IgnoreProtoBundles: tt.ignoreProtoBundles}

			from, err := StringSource(tt.from).DDL(ctx, option)
			if err != nil {
				t.Fatalf("failed to parse from: %v", err)
			}
			to, err := StringSource(tt.to).DDL(ctx, option)
			if err != nil {
				t.Fatalf("failed to parse to: %v", err)
			}

			ddl, err := hammer.Diff(from, to)
			if tt.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
					t.Fatalf("want error containing %q, got %v", tt.wantErr, err)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			want := make([]string, len(tt.expected))
			for i, s := range tt.expected {
				want[i] = strings.TrimPrefix(s, "\n")
			}
			if actual := convertStrings(ddl); !slices.Equal(want, actual) {
				t.Error(formatDDLMismatch(want, actual))
			}

			for _, side := range []struct {
				label string
				ddl   hammer.DDL
			}{{"from", from}, {"to", to}} {
				self, err := hammer.Diff(side.ddl, side.ddl)
				if err != nil {
					t.Errorf("%s -> %s: unexpected error: %v", side.label, side.label, err)
				} else if len(self.List) != 0 {
					t.Errorf("%s -> %s: want no DDL, got%s", side.label, side.label, formatDDLMismatch(nil, convertStrings(self)))
				}
			}
			if _, err := hammer.Diff(to, from); err != nil {
				t.Errorf("to -> from: unexpected error: %v", err)
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
