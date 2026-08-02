package email

import (
	"fmt"
	"net/http"

	"github.com/pablojhp.pergo/internal/channel"
)

func classifyEmailProviderHTTPStatus(provider string, statusCode int) error {
	err := fmt.Errorf("%s API error (http_status=%d)", provider, statusCode)
	if statusCode >= 400 &&
		statusCode < 500 &&
		statusCode != http.StatusTooManyRequests {
		return channel.NewTerminalError(err)
	}
	return err
}
