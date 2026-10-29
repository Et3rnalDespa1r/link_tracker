package main

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"gitlab.education.tbank.ru/backend-academy-go-2025/homeworks/link-tracker/internal/agent/processor"
	"gitlab.education.tbank.ru/backend-academy-go-2025/homeworks/link-tracker/internal/config"
)

func publish(p *kafka.Producer, topic string, value []byte) error {
	delivery := make(chan kafka.Event, 1)
	if err := p.Produce(&kafka.Message{TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny}, Value: value}, delivery); err != nil {
		return err
	}
	select {
	case event := <-delivery:
		msg, ok := event.(*kafka.Message)
		if !ok {
			return fmt.Errorf("unexpected delivery event: %T", event)
		}
		return msg.TopicPartition.Error
	case <-time.After(15 * time.Second):
		return fmt.Errorf("Kafka delivery timeout")
	}
}

func run() error {
	cfg, err := config.Load()
	if err != nil {
		return err
	}
	proc := processor.NewProcessor(cfg.AgentStopWords, cfg.AgentExcludedAuthors, cfg.AgentMinLength, cfg.AgentSummaryThreshold, cfg.AgentHFToken, cfg.AgentHighKeywords, cfg.AgentLowKeywords)
	consumer, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers": strings.Join(cfg.KafkaBrokers, ","), "group.id": cfg.AgentGroupID,
		"auto.offset.reset": "earliest", "enable.auto.commit": false, "enable.auto.offset.store": false,
	})
	if err != nil {
		return err
	}
	defer consumer.Close()
	producer, err := kafka.NewProducer(&kafka.ConfigMap{"bootstrap.servers": strings.Join(cfg.KafkaBrokers, ","), "enable.idempotence": true})
	if err != nil {
		return err
	}
	defer producer.Close()
	if err := consumer.SubscribeTopics([]string{cfg.AgentRawTopic}, nil); err != nil {
		return err
	}
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM)
	defer signal.Stop(signals)
	var pending []*kafka.Message
	var updates []processor.ProcessedUpdate
	deadline := time.Now().Add(cfg.AgentGroupWindow)
	flush := func() error {
		for _, update := range processor.GroupUpdates(updates) {
			payload, err := json.Marshal(update)
			if err != nil {
				return err
			}
			if err := publish(producer, cfg.AgentProcessedTopic, payload); err != nil {
				return err
			}
		}
		// Commit only after every output in this batch has a broker acknowledgement.
		// A crash between publication and commit may replay a batch (at-least-once).
		for _, message := range pending {
			if _, err := consumer.CommitMessage(message); err != nil {
				return err
			}
		}
		pending = nil
		updates = nil
		deadline = time.Now().Add(cfg.AgentGroupWindow)
		return nil
	}
	slog.Info("AI Agent started", "input", cfg.AgentRawTopic, "output", cfg.AgentProcessedTopic)
	for {
		select {
		case <-signals:
			return flush()
		default:
		}
		if time.Now().After(deadline) || len(pending) >= cfg.BatchSize {
			if err := flush(); err != nil {
				return err
			}
		}
		switch event := consumer.Poll(100).(type) {
		case *kafka.Message:
			var raw processor.RawUpdate
			if err := json.Unmarshal(event.Value, &raw); err != nil || len(raw.TgChatIDs) == 0 || raw.Description == "" {
				if err := publish(producer, cfg.AgentRawTopic+".dlq", event.Value); err != nil {
					return err
				}
			} else if processed := proc.ProcessUpdate(raw); processed != nil {
				updates = append(updates, *processed)
			}
			pending = append(pending, event)
		case kafka.Error:
			slog.Error("Kafka consumer error", "error", event)
		}
	}
}

func main() {
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, nil)))
	if err := run(); err != nil {
		slog.Error("AI Agent stopped", "error", err)
		os.Exit(1)
	}
}
