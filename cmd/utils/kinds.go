package utils

import (
	"errors"
	"strings"
)

func NormalizeKind(s string) (string, error) {
	s = strings.TrimSpace(strings.ToLower(s))
	switch s {
	case "auto":
		return "auto", nil
	case "partitioned", "part":
		return "partitioned", nil
	case "non-partitioned", "nonpartitioned", "non_part", "nonpart", "non":
		return "non-partitioned", nil
	default:
		return "", errors.New("invalid --kind (expected auto|partitioned|non-partitioned)")
	}
}
