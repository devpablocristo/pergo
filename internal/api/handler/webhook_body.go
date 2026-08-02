package handler

import (
	"errors"
	"io"
	"net/http"
)

const maxIntegrationWebhookBodyBytes int64 = 2 << 20

var errIntegrationWebhookBodyTooLarge = errors.New("integration webhook body exceeds 2 MiB")

func readIntegrationWebhookBody(request *http.Request) ([]byte, error) {
	body, err := io.ReadAll(io.LimitReader(request.Body, maxIntegrationWebhookBodyBytes+1))
	if err != nil {
		return nil, err
	}
	if int64(len(body)) > maxIntegrationWebhookBodyBytes {
		return nil, errIntegrationWebhookBodyTooLarge
	}
	return body, nil
}
