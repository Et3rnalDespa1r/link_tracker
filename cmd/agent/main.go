package main

import (
	"encoding/json"
	"log/slog"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"gitlab.education.tbank.ru/backend-academy-go-2025/homeworks/link-tracker/internal/agent/processor"
	"gitlab.education.tbank.ru/backend-academy-go-2025/homeworks/link-tracker/internal/config"
)

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))

	cfg, err := config.Load()
	if err != nil {
		logger.Error("Не удалось загрузить конфиг", "error", err)
		os.Exit(1)
	}

	proc := processor.NewProcessor(
		cfg.AgentStopWords,
		cfg.AgentExcludedAuthors,
		cfg.AgentMinLength,
		cfg.AgentSummaryThreshold,
		cfg.AgentHFToken,
		cfg.AgentHighKeywords,
		cfg.AgentLowKeywords,
	)

	grouper := processor.NewGrouper(cfg.AgentGroupWindow)

	brokers := strings.Join(cfg.KafkaBrokers, ",")

	consumer, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers": brokers,
		"group.id":          cfg.AgentGroupID,
		"auto.offset.reset": "earliest",
	})
	if err != nil {
		logger.Error("Ошибка создания Kafka Consumer", "error", err)
		os.Exit(1)
	}
	defer consumer.Close()

	producer, err := kafka.NewProducer(&kafka.ConfigMap{
		"bootstrap.servers": brokers,
	})
	if err != nil {
		logger.Error("Ошибка создания Kafka Producer", "error", err)
		os.Exit(1)
	}
	defer producer.Close()

	err = consumer.SubscribeTopics([]string{cfg.AgentRawTopic}, nil)
	if err != nil {
		logger.Error("Ошибка подписки на топик", "error", err)
		os.Exit(1)
	}

	go func() {
		for groupedBatch := range grouper.Out() {
			for _, msg := range groupedBatch {
				valBytes, err := json.Marshal(msg)
				if err != nil {
					logger.Error("Ошибка сериализации", "error", err)
					continue
				}

				topic := cfg.AgentProcessedTopic
				err = producer.Produce(&kafka.Message{
					TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
					Value:          valBytes,
				}, nil)
				if err != nil {
					logger.Error("Ошибка отправки в Kafka", "error", err)
				}
			}
			producer.Flush(5000)
			logger.Info("Отправлен сгруппированный батч сообщений", "count", len(groupedBatch))
		}
	}()

	sigchan := make(chan os.Signal, 1)
	signal.Notify(sigchan, syscall.SIGINT, syscall.SIGTERM)

	logger.Info("AI Agent запущен", slog.String("topic_in", cfg.AgentRawTopic), slog.String("topic_out", cfg.AgentProcessedTopic))

	run := true
	for run {
		select {
		case sig := <-sigchan:
			logger.Info("Остановка агента по сигналу...", "signal", sig)
			run = false
		default:
			ev := consumer.Poll(100)
			if ev == nil {
				continue
			}

			switch e := ev.(type) {
			case *kafka.Message:
				var raw processor.RawUpdate
				if err := json.Unmarshal(e.Value, &raw); err != nil {
					logger.Error("Некорректный формат сообщения", "error", err, "payload", string(e.Value))
					continue
				}

				processed := proc.ProcessUpdate(raw)
				if processed != nil {
					grouper.Add(*processed)
				} else {
					logger.Info("Обновление отфильтровано", "id", raw.ID)
				}

			case kafka.Error:
				logger.Error("Ошибка Kafka Consumer", "error", e)
			}
		}
	}
}
