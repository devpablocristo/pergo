package config

import "testing"

func TestWhatsAppMockEnabledIsStrictOptIn(t *testing.T) {
	tests := []struct {
		name  string
		value string
		want  bool
	}{
		{name: "unset", value: "", want: false},
		{name: "false", value: "false", want: false},
		{name: "uppercase true", value: "TRUE", want: false},
		{name: "numeric true", value: "1", want: false},
		{name: "exact true", value: "true", want: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Setenv("PERGO_WHATSAPP_MOCK_ENABLED", tt.value)
			if got := Load().WhatsAppMockEnabled; got != tt.want {
				t.Fatalf("WhatsAppMockEnabled = %v, want %v", got, tt.want)
			}
		})
	}
}
