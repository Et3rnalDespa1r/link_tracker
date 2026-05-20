package kafka_test

import (
	"context"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/segmentio/kafka-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	tcKafka "github.com/testcontainers/testcontainers-go/modules/kafka"

	botKafka "gitlab.education.tbank.ru/backend-academy-go-2025/homeworks/link-tracker/internal/infrastructure/kafka"
	scrapperKafka "gitlab.education.tbank.ru/backend-academy-go-2025/homeworks/link-tracker/internal/scrapper/client/kafka"
	scrapDomain "gitlab.education.tbank.ru/backend-academy-go-2025/homeworks/link-tracker/internal/scrapper/domain"
)

type MockTelegramBot struct {
	Received []string
}

func (m *MockTelegramBot) SendNotification(ctx context.Context, chatID int64, text string) error {
	m.Received = append(m.Received, text)
	return nil
}

func TestE2E_ScraperToBot_ViaKafka(t *testing.T) {
	ctx := context.Background()

	kafkaContainer, err := tcKafka.RunContainer(ctx, tcKafka.WithClusterID("e2e-cluster"))
	require.NoError(t, err)
	defer kafkaContainer.Terminate(ctx)

	brokers, err := kafkaContainer.Brokers(ctx)
	require.NoError(t, err)

	topic := "link.updates.e2e"
	dlqTopic := "link.updates.e2e.dlq"

	conn, err := kafka.Dial("tcp", brokers[0])
	require.NoError(t, err)
	controller, err := conn.Controller()
	require.NoError(t, err)
	controllerConn, err := kafka.Dial("tcp", net.JoinHostPort(controller.Host, strconv.Itoa(controller.Port)))
	require.NoError(t, err)
	_ = controllerConn.CreateTopics(
		kafka.TopicConfig{Topic: topic, NumPartitions: 1, ReplicationFactor: 1},
		kafka.TopicConfig{Topic: dlqTopic, NumPartitions: 1, ReplicationFactor: 1},
	)
	controllerConn.Close()
	conn.Close()
	time.Sleep(1 * time.Second)

	mockSR := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/vnd.schemaregistry.v1+json")
		if r.Method == http.MethodPost {
			w.Write([]byte(`{"id": 1}`))
			return
		}
		w.Write([]byte(`{"schema":"{\"type\":\"record\",\"name\":\"LinkUpdateEvent\",\"namespace\":\"com.example.notification\",\"fields\":[{\"name\":\"id\",\"type\":\"long\"},{\"name\":\"url\",\"type\":\"string\"},{\"name\":\"description\",\"type\":\"string\"},{\"name\":\"tgChatIds\",\"type\":{\"type\":\"array\",\"items\":\"long\"}}]}"}`))
	}))
	defer mockSR.Close()

	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	mockTgBot := &MockTelegramBot{}
	consumer := botKafka.NewConsumer(
		brokers,
		topic,
		dlqTopic,
		"e2e-group",
		logger,
		3,
		mockTgBot,
		mockSR.URL,
	)

	ctxCancel, cancel := context.WithCancel(ctx)
	defer cancel()
	go consumer.Start(ctxCancel)
	defer consumer.Close()

	producer, err := scrapperKafka.NewProducer(brokers, topic, mockSR.URL)
	require.NoError(t, err)
	defer producer.Close()

	update := scrapDomain.LinkUpdate{
		ID:          42,
		URL:         "https://github.com/T-Academy/e2e-test",
		Description: "E2E Test Success",
		TgChatIDs:   []int64{12345},
	}

	err = producer.SendUpdate(ctx, update)
	require.NoError(t, err)

	assert.Eventually(t, func() bool {
		return len(mockTgBot.Received) > 0
	}, 20*time.Second, 1*time.Second)
}
