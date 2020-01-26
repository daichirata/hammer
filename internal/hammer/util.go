package hammer

import (
	"strings"
)

func Scheme(uri string) string {
	if i := strings.Index(uri, ":"); i > 0 {
		return strings.ToLower(uri[0:i])
	}
	return ""
}
