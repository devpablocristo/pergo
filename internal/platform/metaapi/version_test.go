package metaapi

import "testing"

func TestGraphVersionPolicy(t *testing.T) {
	if err := ValidateVersion(DefaultVersion); err != nil {
		t.Fatalf("ValidateVersion(%q): %v", DefaultVersion, err)
	}
	for _, version := range []string{
		"",
		"18.0",
		"v18.0",
		"v20.0",
		"v24.0",
		"v25.1",
		"v26.0",
		"v27.0",
		"latest",
	} {
		if err := ValidateVersion(version); err == nil {
			t.Fatalf("ValidateVersion(%q) accepted an unsupported or ambiguous value", version)
		}
	}
	if got := BaseURL(DefaultVersion); got != "https://graph.facebook.com/v25.0" {
		t.Fatalf("BaseURL = %q", got)
	}
}
