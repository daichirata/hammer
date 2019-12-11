package internal_test

import (
	"testing"

	"github.com/daichirata/hammer/internal"
)

func TestScheme(t *testing.T) {
	values := []struct {
		u string
		s string
	}{
		{
			u: "spanner://projects/projectId/instances/instanceId/databases/databaseName?credentials=/path/to/file.json",
			s: "spanner",
		},
		{
			u: "file:///path/to/file",
			s: "file",
		},
		{
			u: "./path/to/file",
			s: "",
		},
		{
			u: "/path/to/file",
			s: "",
		},
	}
	for _, v := range values {
		actual := internal.Scheme(v.u)

		if actual != v.s {
			t.Errorf("got: %v, want: %v", actual, v.s)
		}
	}
}
