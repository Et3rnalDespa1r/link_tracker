package kafka_test

import (
	"context"
	"errors"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/segmentio/kafka-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	tcKafka "github.com/testcontainers/testcontainers-go/modules/kafka"

	myKafka "gitlab.education.tbank.ru/backend-academy-go-2025/homeworks/link-tracker/internal/infrastructure/kafka"
	scrapperKafka "gitlab.education.tbank.ru/backend-academy-go-2025/homeworks/link-tracker/internal/scrapper/client/kafka"
	scrapDomain "gitlab.education.tbank.ru/backend-academy-go-2025/homeworks/link-tracker/internal/scrapper/domain"
)

type MockBot struct {
	mu          sync.Mutex
	calls       int
	shouldError bool
}

func (m *MockBot) SendNotification(ctx context.Context, chatID int64, text string) error {
	m.mu.Lock()
	m.calls++
	m.mu.Unlock()
	if m.shouldError {
		return errors.New("network error")
	}
	return nil
}

func (m *MockBot) GetCalls() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.calls
}

func TestConsumer_Integration(t *testing.T) {
	ctx := context.Background()

	kafkaContainer, err := tcKafka.RunContainer(ctx, tcKafka.WithClusterID("test-cluster"))
	require.NoError(t, err)
	defer kafkaContainer.Terminate(ctx)

	brokers, err := kafkaContainer.Brokers(ctx)
	require.NoError(t, err)

	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))

	createTopic := func(t *testing.T, topic string) {
		conn, err := kafka.Dial("tcp", brokers[0])
		require.NoError(t, err)
		defer conn.Close()

		controller, err := conn.Controller()
		require.NoError(t, err)
		controllerConn, err := kafka.Dial("tcp", net.JoinHostPort(controller.Host, strconv.Itoa(controller.Port)))
		require.NoError(t, err)
		defer controllerConn.Close()

		_ = controllerConn.CreateTopics(kafka.TopicConfig{
			Topic:             topic,
			NumPartitions:     1,
			ReplicationFactor: 1,
		})
		time.Sleep(1 * time.Second)
	}

	readFromDLQ := func(t *testing.T, dlqTopic string) []byte {
		r := kafka.NewReader(kafka.ReaderConfig{
			Brokers:     brokers,
			Topic:       dlqTopic,
			Partition:   0,
			MinBytes:    1,
			MaxBytes:    10e6,
			StartOffset: kafka.FirstOffset,
		})
		defer r.Close()

		var lastMsg []byte
		assert.Eventually(t, func() bool {
			ctxTimeout, cancel := context.WithTimeout(ctx, 3*time.Second)
			defer cancel()
			msg, err := r.ReadMessage(ctxTimeout)
			if err != nil {
				return false
			}
			lastMsg = msg.Value
			return true
		}, 45*time.Second, 1*time.Second)
		return lastMsg
	}

	mockSR := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/vnd.schemaregistry.v1+json")
		if r.Method == http.MethodPost {
			w.Write([]byte(`{"id": 1}`))
			return
		}
		w.Write([]byte(`{"schema":"{\"type\":\"record\",\"name\":\"LinkUpdateEvent\",\"namespace\":\"com.example.notification\",\"fields\":[{\"name\":\"id\",\"type\":\"long\"},{\"name\":\"url\",\"type\":\"string\"},{\"name\":\"description\",\"type\":\"string\"},{\"name\":\"tgChatIds\",\"type\":{\"type\":\"array\",\"items\":\"long\"}}]}"}`))
	}))
	defer mockSR.Close()

	t.Run("Valid message -> successful processing", func(t *testing.T) {
		topic, dlqTopic := "updates.happy", "updates.happy.dlq"
		createTopic(t, topic)
		createTopic(t, dlqTopic)

		mockBot := &MockBot{shouldError: false}
		consumer := myKafka.NewConsumer(brokers, topic, dlqTopic, "g1", logger, 3, mockBot, mockSR.URL)

		ctxCancel, cancel := context.WithCancel(ctx)
		defer cancel()
		go consumer.Start(ctxCancel)
		defer consumer.Close()

		producer, _ := scrapperKafka.NewProducer(brokers, topic, mockSR.URL)
		defer producer.Close()
		_ = producer.SendUpdate(ctx, scrapDomain.LinkUpdate{URL: "https://ok", Description: "Test", TgChatIDs: []int64{123}})

		assert.Eventually(t, func() bool {
			return mockBot.GetCalls() >= 1
		}, 30*time.Second, 500*time.Millisecond)
	})

	t.Run("Invalid message -> sent to DLQ", func(t *testing.T) {
		topic, dlqTopic := "updates.bad", "updates.bad.dlq"
		createTopic(t, topic)
		createTopic(t, dlqTopic)

		mockBot := &MockBot{shouldError: false}
		consumer := myKafka.NewConsumer(brokers, topic, dlqTopic, "g2", logger, 3, mockBot, mockSR.URL)

		ctxCancel, cancel := context.WithCancel(ctx)
		defer cancel()
		go consumer.Start(ctxCancel)
		defer consumer.Close()

		badMsg := []byte("broken_json_here")
		w := &kafka.Writer{Addr: kafka.TCP(brokers...), Topic: topic, MaxAttempts: 5, RequiredAcks: kafka.RequireAll}
		_ = w.WriteMessages(ctx, kafka.Message{Value: badMsg})
		w.Close()

		dlqMsg := readFromDLQ(t, dlqTopic)
		assert.Equal(t, string(badMsg), string(dlqMsg))
		assert.Equal(t, 0, mockBot.GetCalls())
	})

	t.Run("Error processing -> retries exhausted -> sent to DLQ", func(t *testing.T) {
		topic, dlqTopic := "updates.retry", "updates.retry.dlq"
		createTopic(t, topic)
		createTopic(t, dlqTopic)

		mockBot := &MockBot{shouldError: true}
		consumer := myKafka.NewConsumer(brokers, topic, dlqTopic, "g3", logger, 3, mockBot, mockSR.URL)

		ctxCancel, cancel := context.WithCancel(ctx)
		defer cancel()
		go consumer.Start(ctxCancel)
		defer consumer.Close()

		producer, _ := scrapperKafka.NewProducer(brokers, topic, mockSR.URL)
		defer producer.Close()
		_ = producer.SendUpdate(ctx, scrapDomain.LinkUpdate{URL: "https://fail", Description: "Fail", TgChatIDs: []int64{123}})

		dlqMsg := readFromDLQ(t, dlqTopic)
		assert.NotEmpty(t, dlqMsg)

		assert.Eventually(t, func() bool {
			return mockBot.GetCalls() == 3
		}, 30*time.Second, 1*time.Second)
	})
}
