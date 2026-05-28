package agent_test

import (
	"context"
	"encoding/json"
	"testing"
	"time"

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

	producer, _ := kafka.NewProducer(&kafka.ConfigMap{"bootstrap.servers": brokers[0]})

	valSpam, _ := json.Marshal(processor.RawUpdate{ID: 1, Description: "это spam сообщение", TgChatIDs: []int64{1}})
	producer.Produce(&kafka.Message{TopicPartition: kafka.TopicPartition{Topic: &rawTopic, Partition: kafka.PartitionAny}, Value: valSpam}, nil)

	val1, _ := json.Marshal(processor.RawUpdate{ID: 2, Description: "нормальное сообщение 1", TgChatIDs: []int64{2}})
	val2, _ := json.Marshal(processor.RawUpdate{ID: 3, Description: "critical баг пофикшен", TgChatIDs: []int64{2}})
	producer.Produce(&kafka.Message{TopicPartition: kafka.TopicPartition{Topic: &rawTopic, Partition: kafka.PartitionAny}, Value: val1}, nil)
	producer.Produce(&kafka.Message{TopicPartition: kafka.TopicPartition{Topic: &rawTopic, Partition: kafka.PartitionAny}, Value: val2}, nil)

	producer.Flush(5000)
	producer.Close()

	proc := processor.NewProcessor([]string{"spam"}, nil, 5, 100, "", []string{"critical"}, []string{"typo"})
	grouper := processor.NewGrouper(100 * time.Millisecond)

	agentConsumer, _ := kafka.NewConsumer(&kafka.ConfigMap{"bootstrap.servers": brokers[0], "group.id": "test-agent", "auto.offset.reset": "earliest"})
	agentConsumer.SubscribeTopics([]string{rawTopic}, nil)
	agentProducer, _ := kafka.NewProducer(&kafka.ConfigMap{"bootstrap.servers": brokers[0]})

	go func() {
		for grouped := range grouper.Out() {
			for _, msg := range grouped {
				outBytes, _ := json.Marshal(msg)
				agentProducer.Produce(&kafka.Message{TopicPartition: kafka.TopicPartition{Topic: &processedTopic, Partition: kafka.PartitionAny}, Value: outBytes}, nil)
			}
			agentProducer.Flush(5000)
		}
	}()
	for i := 0; i < 20; i++ {
		ev := agentConsumer.Poll(1000)
		if msg, ok := ev.(*kafka.Message); ok {
			var raw processor.RawUpdate
			if err := json.Unmarshal(msg.Value, &raw); err == nil {
				if processed := proc.ProcessUpdate(raw); processed != nil {
					grouper.Add(*processed)
				}
			}
		}
	}
	time.Sleep(300 * time.Millisecond)
	agentConsumer.Close()
	agentProducer.Close()

	botConsumer, _ := kafka.NewConsumer(&kafka.ConfigMap{"bootstrap.servers": brokers[0], "group.id": "test-bot", "auto.offset.reset": "earliest"})
	botConsumer.SubscribeTopics([]string{processedTopic}, nil)

	var results []processor.ProcessedUpdate
	for i := 0; i < 20; i++ {
		ev := botConsumer.Poll(1000)
		if msg, ok := ev.(*kafka.Message); ok {
			var r processor.ProcessedUpdate
			json.Unmarshal(msg.Value, &r)
			results = append(results, r)
		}
	}
	botConsumer.Close()

	require.Len(t, results, 1, "Должно прийти только одно сгруппированное сообщение (спам отсекается)")
	assert.Equal(t, processor.PriorityHigh, results[0].Priority)
	assert.Contains(t, results[0].Description, "1. нормальное сообщение 1")
	assert.Contains(t, results[0].Description, "2. critical баг пофикшен")
}
