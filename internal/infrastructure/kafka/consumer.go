package kafka

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/hamba/avro/v2"
	"github.com/riferrei/srclient"
	"github.com/segmentio/kafka-go"
	"gitlab.education.tbank.ru/backend-academy-go-2025/homeworks/link-tracker/internal/domain"
)

const (
	magicByte        = 0
	magicByteSize    = 1
	schemaIDSize     = 4
	avroHeaderLength = magicByteSize + schemaIDSize
)

type NotificationSender interface {
	SendNotification(ctx context.Context, chatID int64, text string) error
}

type Consumer struct {
	reader    *kafka.Reader
	dlqWriter *kafka.Writer
	srClient  *srclient.SchemaRegistryClient
	logger    *slog.Logger
	bot       NotificationSender
	retries   int
	schemas   map[int]avro.Schema
	mu        sync.RWMutex
}

func NewConsumer(brokers []string, topic, dlqTopic, groupID string, logger *slog.Logger, retries int, bot NotificationSender, registryURL string) *Consumer {
	return &Consumer{
		reader: kafka.NewReader(kafka.ReaderConfig{
			Brokers: brokers,
			Topic:   topic,
			GroupID: groupID,
		}),
		dlqWriter: &kafka.Writer{
			Addr:  kafka.TCP(brokers...),
			Topic: dlqTopic,
		},
		srClient: srclient.CreateSchemaRegistryClient(registryURL),
		logger:   logger,
		bot:      bot,
		retries:  retries,
		schemas:  make(map[int]avro.Schema),
	}
}

func (c *Consumer) Start(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		default:
			msg, err := c.reader.FetchMessage(ctx)
			if err != nil {
				if errors.Is(err, context.Canceled) {
					return
				}
				continue
			}

			if err := c.processMessage(ctx, msg); err != nil {
				c.logger.Error("Failed to process message", "error", err)
				c.sendToDLQ(ctx, msg)
			}

			c.reader.CommitMessages(ctx, msg)
		}
	}
}

func (c *Consumer) processMessage(ctx context.Context, msg kafka.Message) error {
	if len(msg.Value) < avroHeaderLength || msg.Value[0] != magicByte {
		return fmt.Errorf("invalid magic byte")
	}

	schemaID := int(binary.BigEndian.Uint32(msg.Value[magicByteSize:avroHeaderLength]))
	parsedSchema, err := c.getSchema(schemaID)
	if err != nil {
		return err
	}

	var update domain.LinkUpdate
	if err := avro.Unmarshal(parsedSchema, msg.Value[avroHeaderLength:], &update); err != nil {
		return err
	}

	var lastErr error
	for i := 0; i < c.retries; i++ {
		successCount := 0
		for _, chatID := range update.TgChatIDs {
			if err := c.bot.SendNotification(ctx, chatID, update.Description); err != nil {
				lastErr = err
			} else {
				successCount++
			}
		}

		if successCount == len(update.TgChatIDs) {
			return nil
		}

		time.Sleep(time.Second * time.Duration(i+1))
	}

	return fmt.Errorf("retries exhausted: %w", lastErr)
}

func (c *Consumer) sendToDLQ(ctx context.Context, msg kafka.Message) {
	err := c.dlqWriter.WriteMessages(ctx, kafka.Message{
		Value: msg.Value,
		Key:   msg.Key,
	})
	if err != nil {
		c.logger.Error("Failed to send to DLQ", "error", err)
	}
}

func (c *Consumer) getSchema(id int) (avro.Schema, error) {
	c.mu.RLock()
	if s, ok := c.schemas[id]; ok {
		c.mu.RUnlock()
		return s, nil
	}
	c.mu.RUnlock()

	schema, err := c.srClient.GetSchema(id)
	if err != nil {
		return nil, err
	}

	parsed, err := avro.Parse(schema.Schema())
	if err != nil {
		return nil, err
	}

	c.mu.Lock()
	c.schemas[id] = parsed
	c.mu.Unlock()

	return parsed, nil
}

func (c *Consumer) Close() error {
	_ = c.dlqWriter.Close()
	return c.reader.Close()
}
