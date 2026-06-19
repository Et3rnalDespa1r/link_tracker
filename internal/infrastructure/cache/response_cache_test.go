package cache

import (
	"context"
	"testing"
	"time"

	"github.com/redis/rueidis"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

func setupValkeyContainer(ctx context.Context, t *testing.T) (rueidis.Client, func()) {
	req := testcontainers.ContainerRequest{
		Image:        "valkey/valkey:8.0",
		ExposedPorts: []string{"6379/tcp"},
		WaitingFor:   wait.ForLog("Ready to accept connections"),
	}

	valkeyC, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
	require.NoError(t, err)

	endpoint, err := valkeyC.Endpoint(ctx, "")
	require.NoError(t, err)

	client, err := rueidis.NewClient(rueidis.ClientOption{
		InitAddress: []string{endpoint},
	})
	require.NoError(t, err)

	cleanup := func() {
		client.Close()
		_ = valkeyC.Terminate(ctx)
	}

	return client, cleanup
}

func TestResponseCache_Integration(t *testing.T) {
	ctx := context.Background()
	client, cleanup := setupValkeyContainer(ctx, t)
	defer cleanup()

	cacheTTL := 2 * time.Second
	respCache := NewResponseCache(client, cacheTTL)
	chatID := int64(12345)
	testData := []byte(`{"links":[{"id":1,"url":"https://github.com"}],"size":1}`)

	t.Run("Get empty cache", func(t *testing.T) {
		data, err := respCache.GetList(ctx, chatID)
		assert.NoError(t, err)
		assert.Nil(t, data, "Ожидаем nil для пустого кэша")
	})

	t.Run("Set and Get cache", func(t *testing.T) {
		err := respCache.SetList(ctx, chatID, testData)
		assert.NoError(t, err)

		data, err := respCache.GetList(ctx, chatID)
		assert.NoError(t, err)
		assert.JSONEq(t, string(testData), string(data))
	})

	t.Run("Invalidate cache", func(t *testing.T) {
		err := respCache.Invalidate(ctx, chatID)
		assert.NoError(t, err)

		data, err := respCache.GetList(ctx, chatID)
		assert.NoError(t, err)
		assert.Nil(t, data, "Ожидаем nil после инвалидации")
	})

	t.Run("TTL expiration", func(t *testing.T) {
		err := respCache.SetList(ctx, chatID, testData)
		assert.NoError(t, err)
		
		time.Sleep(3 * time.Second)

		data, err := respCache.GetList(ctx, chatID)
		assert.NoError(t, err)
		assert.Nil(t, data, "Ожидаем nil после истечения TTL")
	})
}
