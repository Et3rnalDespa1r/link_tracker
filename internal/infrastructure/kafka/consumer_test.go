package kafka_test

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/segmentio/kafka-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	tcKafka "github.com/testcontainers/testcontainers-go/modules/kafka"

	"gitlab.education.tbank.ru/backend-academy-go-2025/homeworks/link-tracker/internal/domain"
	myKafka "gitlab.education.tbank.ru/backend-academy-go-2025/homeworks/link-tracker/internal/infrastructure/kafka"
)

type MockBot struct {
	calls       int
	shouldError bool
}

func (m *MockBot) SendNotification(ctx context.Context, chatID int64, text string) error {
	m.calls++
	if m.shouldError {
		return errors.New("ошибка сети телеграма")
	}
	return nil
}

func TestConsumer_Integration(t *testing.T) {
	ctx := context.Background()

	kafkaContainer, err := tcKafka.RunContainer(ctx, tcKafka.WithClusterID("test-cluster"))
	require.NoError(t, err, "Не удалось поднять Kafka Testcontainer")

	defer func() {
		if err := kafkaContainer.Terminate(ctx); err != nil {
			t.Fatalf("Ошибка при остановке контейнера: %s", err)
		}
	}()

	brokers, err := kafkaContainer.Brokers(ctx)
	require.NoError(t, err)

	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))

	createTopic := func(t *testing.T, topic string) {
		conn, err := kafka.Dial("tcp", brokers[0])
		require.NoError(t, err)
		defer conn.Close()

		controller, err := conn.Controller()
		require.NoError(t, err)
		var controllerConn *kafka.Conn
		controllerConn, err = kafka.Dial("tcp", net.JoinHostPort(controller.Host, strconv.Itoa(controller.Port)))
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
		}, 20*time.Second, 500*time.Millisecond, "Не удалось прочитать сообщение из DLQ")

		return lastMsg
	}
	writeToKafka := func(t *testing.T, topic string, value []byte) {
		w := &kafka.Writer{
			Addr:        kafka.TCP(brokers...),
			Topic:       topic,
			MaxAttempts: 5,
		}
		defer w.Close()

		var err error
		for i := 0; i < 10; i++ {
			err = w.WriteMessages(ctx, kafka.Message{Value: value})
			if err == nil {
				return
			}
			time.Sleep(1 * time.Second)
		}
		require.NoError(t, err, "Не удалось записать сообщение в топик")
	}

	t.Run("Valid message -> successful processing", func(t *testing.T) {
		topic, dlqTopic := "updates.happy", "updates.happy.dlq"
		createTopic(t, topic)
		createTopic(t, dlqTopic)

		mockBot := &MockBot{shouldError: false}
		consumer := myKafka.NewConsumer(brokers, topic, dlqTopic, "g1", 3, mockBot, logger)

		ctxCancel, cancel := context.WithCancel(ctx)
		defer cancel()
		go consumer.Start(ctxCancel)
		defer consumer.Close()

		update := domain.LinkUpdate{URL: "https://github.com/ok", Description: "Test", TgChatIDs: []int64{123}}
		b, _ := json.Marshal(update)
		writeToKafka(t, topic, b)

		assert.Eventually(t, func() bool {
			return mockBot.calls >= 1
		}, 15*time.Second, 500*time.Millisecond, "Бот должен был отправиться 1 раз")
	})

	t.Run("Invalid message -> sent to DLQ", func(t *testing.T) {
		topic, dlqTopic := "updates.bad", "updates.bad.dlq"
		createTopic(t, topic)
		createTopic(t, dlqTopic)

		mockBot := &MockBot{shouldError: false}
		consumer := myKafka.NewConsumer(brokers, topic, dlqTopic, "g2", 3, mockBot, logger)

		ctxCancel, cancel := context.WithCancel(ctx)
		defer cancel()
		go consumer.Start(ctxCancel)
		defer consumer.Close()

		badMsg := []byte("broken_json_here")
		writeToKafka(t, topic, badMsg)

		dlqMsg := readFromDLQ(t, dlqTopic)
		assert.Equal(t, string(badMsg), string(dlqMsg), "В DLQ должен лежать тот же кривой текст")
		assert.Equal(t, 0, mockBot.calls, "Бот не должен был вызываться")
	})

	t.Run("Error processing -> retries exhausted -> sent to DLQ", func(t *testing.T) {
		topic, dlqTopic := "updates.retry", "updates.retry.dlq"
		createTopic(t, topic)
		createTopic(t, dlqTopic)

		mockBot := &MockBot{shouldError: true}
		consumer := myKafka.NewConsumer(brokers, topic, dlqTopic, "g3", 3, mockBot, logger)

		ctxCancel, cancel := context.WithCancel(ctx)
		defer cancel()
		go consumer.Start(ctxCancel)
		defer consumer.Close()

		update := domain.LinkUpdate{URL: "https://github.com/fail", Description: "Fail", TgChatIDs: []int64{123}}
		b, _ := json.Marshal(update)
		writeToKafka(t, topic, b)

		dlqMsg := readFromDLQ(t, dlqTopic)
		assert.JSONEq(t, string(b), string(dlqMsg), "Оригинальный JSON должен дойти до DLQ")
		assert.Equal(t, 3, mockBot.calls, "Должно быть ровно 3 попытки отправить сообщение")
	})
}
