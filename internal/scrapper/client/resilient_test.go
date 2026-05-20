package client

import (
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"gitlab.education.tbank.ru/backend-academy-go-2025/homeworks/link-tracker/internal/config"
)

const (
	testHTTPTimeout        = 1 * time.Second
	testRetryAttempts      = 3
	testRetryDelay         = 10 * time.Millisecond
	testCBMinCalls         = 5
	testCBFailureThreshold = 80.0
)

func TestResilientClient_RetryAndTimeout(t *testing.T) {
	var callCount int32

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		count := atomic.AddInt32(&callCount, 1)
		if count <= 2 {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	cfg := &config.Config{
		HTTPTimeout:            testHTTPTimeout,
		RetryAttempts:          testRetryAttempts,
		RetryDelay:             testRetryDelay,
		RetryableStatuses:      []int{500},
		CBMinimumRequiredCalls: testCBMinCalls,
		CBFailureRateThreshold: testCBFailureThreshold,
	}

	resilientClient := NewResilientHTTPClient(cfg, "test_client")

	req, _ := http.NewRequest("GET", server.URL, nil)
	resp, err := resilientClient.Do(req)

	assert.NoError(t, err)
	assert.Equal(t, http.StatusOK, resp.StatusCode)
	assert.Equal(t, int32(3), atomic.LoadInt32(&callCount))
}
