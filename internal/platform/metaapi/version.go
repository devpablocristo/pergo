// Package metaapi centralizes the versioned Graph API endpoint used by every
// Meta-backed adapter.
package metaapi

import (
	"fmt"
)

// DefaultVersion is deliberately one release behind the newest Graph API
// version: v25.0 is supported until 2028-07-29, while v26.0 was released on
// 2026-07-29 and still has no published retirement date.
// Source: https://developers.facebook.com/docs/graph-api/changelog/
const DefaultVersion = "v25.0"

func ValidateVersion(version string) error {
	if version != DefaultVersion {
		return fmt.Errorf(
			"must be the audited release %s; update the adapter and tests before changing it",
			DefaultVersion,
		)
	}
	return nil
}

func BaseURL(version string) string {
	return "https://graph.facebook.com/" + version
}
