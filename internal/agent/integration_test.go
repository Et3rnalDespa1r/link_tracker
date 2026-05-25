package agent_test

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	tckafka "github.com/testcontainers/testcontainers-go/modules/kafka"
	"gitlab.education.tbank.ru/backend-academy-go-2025/homeworks/link-tracker/internal/agent/processor"
)

func TestKafkaAgentIntegration(t *testing.T) {
	ctx := context.Background()

	kafkaContainer, err := tckafka.RunContainer(ctx, testcontainers.WithImage("confluentinc/confluent-local:7.5.0"))
	require.NoError(t, err)
	defer kafkaContainer.Terminate(ctx)

	brokers, err := kafkaContainer.Brokers(ctx)
	require.NoError(t, err)

	rawTopic := "link.raw-updates"
	processedTopic := "link.processed-updates"
	
	producer, err := kafka.NewProducer(&kafka.ConfigMap{"bootstrap.servers": brokers[0]})
	require.NoError(t, err)

	_ = producer.Produce(&kafka.Message{TopicPartition: kafka.TopicPartition{Topic: &rawTopic, Partition: kafka.PartitionAny}, Value: []byte("invalid json data")}, nil)

	validUpdate := processor.RawUpdate{ID: 1, Description: "Отличное обновление", Author: "good_user", TgChatIDs: []int64{123}}
	valBytes, _ := json.Marshal(validUpdate)
	_ = producer.Produce(&kafka.Message{TopicPartition: kafka.TopicPartition{Topic: &rawTopic, Partition: kafka.PartitionAny}, Value: valBytes}, nil)

	producer.Flush(5000)
	producer.Close()

	proc := processor.NewProcessor([]string{"spam"}, []string{"bad_user"}, 10, 100, "")

	agentConsumer, _ := kafka.NewConsumer(&kafka.ConfigMap{"bootstrap.servers": brokers[0], "group.id": "test-agent", "auto.offset.reset": "earliest"})
	agentConsumer.SubscribeTopics([]string{rawTopic}, nil)

	agentProducer, _ := kafka.NewProducer(&kafka.ConfigMap{"bootstrap.servers": brokers[0]})

	agentProcessed := false
	for i := 0; i < 20; i++ {
		ev := agentConsumer.Poll(1000)
		if msg, ok := ev.(*kafka.Message); ok {
			var raw processor.RawUpdate
			if err := json.Unmarshal(msg.Value, &raw); err == nil {
				if processed := proc.ProcessUpdate(raw); processed != nil {
					outBytes, _ := json.Marshal(processed)
					_ = agentProducer.Produce(&kafka.Message{TopicPartition: kafka.TopicPartition{Topic: &processedTopic, Partition: kafka.PartitionAny}, Value: outBytes}, nil)
					agentProcessed = true
					break
				}
			}
		}
	}
	agentProducer.Flush(5000)
	require.True(t, agentProcessed, "Агент не смог прочитать или обработать сообщение")

	agentConsumer.Close()
	agentProducer.Close()

	botConsumer, _ := kafka.NewConsumer(&kafka.ConfigMap{"bootstrap.servers": brokers[0], "group.id": "test-bot", "auto.offset.reset": "earliest"})
	botConsumer.SubscribeTopics([]string{processedTopic}, nil)

	var result processor.ProcessedUpdate
	botFound := false
	for i := 0; i < 20; i++ {
		ev := botConsumer.Poll(1000)
		if msg, ok := ev.(*kafka.Message); ok {
			if err := json.Unmarshal(msg.Value, &result); err == nil {
				botFound = true
				break
			}
		}
	}
	require.True(t, botFound, "Бот не смог прочитать финальное сообщение")
	botConsumer.Close()

	assert.Equal(t, validUpdate.ID, result.ID)
	assert.Equal(t, validUpdate.Description, result.Description)
	assert.Equal(t, "HIGH", result.Priority)
}
