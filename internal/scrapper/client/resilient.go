package client

import (
	"bytes"
	"context"
	"fmt"
	"gitlab.education.tbank.ru/backend-academy-go-2025/homeworks/link-tracker/internal/metrics"
	"io"
	"net/http"
	"time"

	"github.com/avast/retry-go/v4"
	"github.com/sony/gobreaker"
	"gitlab.education.tbank.ru/backend-academy-go-2025/homeworks/link-tracker/internal/config"
)

type ResilientHTTPClient struct {
	name   string
	client *http.Client
	cfg    *config.Config
	cb     *gobreaker.CircuitBreaker
}

func NewResilientHTTPClient(cfg *config.Config, name string) *ResilientHTTPClient {
	var st gobreaker.Settings
	st.Name = name
	st.MaxRequests = cfg.CBPermittedCallsInHalfOpen
	st.Timeout = cfg.CBWaitDurationInOpenState

	st.ReadyToTrip = func(counts gobreaker.Counts) bool {
		if counts.Requests < cfg.CBMinimumRequiredCalls {
			return false
		}

		failureRate := float64(counts.TotalFailures) / float64(counts.Requests) * 100
		return failureRate >= cfg.CBFailureRateThreshold
	}

	return &ResilientHTTPClient{
		name:   name,
		client: &http.Client{},
		cfg:    cfg,
		cb:     gobreaker.NewCircuitBreaker(st),
	}
}

func (c *ResilientHTTPClient) isRetryable(code int) bool {
	for _, s := range c.cfg.RetryableStatuses {
		if code == s {
			return true
		}
	}
	return false
}

func (c *ResilientHTTPClient) Do(req *http.Request) (*http.Response, error) {
	started := time.Now()
	defer func() {
		metrics.RequestDuration.WithLabelValues("external_source", c.name).Observe(float64(time.Since(started).Microseconds()) / 1000)
	}()
	var finalResp *http.Response

	retryOpts := []retry.Option{
		retry.Attempts(uint(c.cfg.RetryAttempts)),
		retry.Context(req.Context()),
		retry.RetryIf(func(err error) bool {
			if err == gobreaker.ErrOpenState {
				return false
			}
			return true
		}),
	}

	if c.cfg.RetryExponential {
		retryOpts = append(retryOpts, retry.DelayType(retry.BackOffDelay), retry.Delay(c.cfg.RetryDelay))
	} else {
		retryOpts = append(retryOpts, retry.DelayType(retry.FixedDelay), retry.Delay(c.cfg.RetryDelay))
	}

	err := retry.Do(func() error {
		ctx, cancel := context.WithTimeout(req.Context(), c.cfg.HTTPTimeout)
		defer cancel()
		reqWithCtx := req.Clone(ctx)

		respInterface, cbErr := c.cb.Execute(func() (interface{}, error) {
			resp, err := c.client.Do(reqWithCtx)
			if err != nil {
				return nil, err
			}

			if c.isRetryable(resp.StatusCode) {
				io.Copy(io.Discard, resp.Body)
				resp.Body.Close()
				return nil, fmt.Errorf("retryable http status: %d", resp.StatusCode)
			}

			data, err := io.ReadAll(io.LimitReader(resp.Body, 4<<20))
			resp.Body.Close()
			if err != nil {
				return nil, err
			}
			resp.Body = io.NopCloser(bytes.NewReader(data))
			return resp, nil
		})

		if cbErr != nil {
			return cbErr
		}

		finalResp = respInterface.(*http.Response)
		return nil
	}, retryOpts...)

	if err != nil {
		return nil, err
	}

	return finalResp, nil
}
