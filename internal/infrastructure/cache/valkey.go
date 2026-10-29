package cache

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"time"

	"github.com/redis/rueidis"
)

const (
	DialTimeout    = 5 * time.Second
	PingTimeout    = 5 * time.Second
	InitRetryCount = 6
	RetryDelay     = 2 * time.Second
)

func NewValkeyClient(addresses []string, logger *slog.Logger) (rueidis.Client, error) {
	var lastErr error

	for attempt := 1; attempt <= InitRetryCount; attempt++ {
		client, err := rueidis.NewClient(rueidis.ClientOption{
			InitAddress: addresses,
			Dialer: net.Dialer{
				Timeout: DialTimeout,
			},
		})
		if err != nil {
			lastErr = err
			logger.Warn("Ошибка подключения к Valkey, повторная попытка", "attempt", attempt, "error", err)
			if attempt < InitRetryCount {
				time.Sleep(RetryDelay)
				continue
			}
			return nil, fmt.Errorf("ошибка подключения к Valkey: %w", err)
		}

		ctx, cancel := context.WithTimeout(context.Background(), PingTimeout)
		err = client.Do(ctx, client.B().Ping().Build()).Error()
		cancel()
		if err == nil {
			logger.Info("Успешно подключились к кластеру Valkey (Client-Side Caching включен)", "addresses", addresses)
			return client, nil
		}

		client.Close()
		lastErr = err
		logger.Warn("Не удалось выполнить PING Valkey, повторная попытка", "attempt", attempt, "error", err)
		if attempt < InitRetryCount {
			time.Sleep(RetryDelay)
		}
	}

	return nil, fmt.Errorf("не удалось подключиться к Valkey после %d попыток: %w", InitRetryCount, lastErr)
}
