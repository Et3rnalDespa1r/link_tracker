package kafka

import (
	"context"
	"encoding/binary"
	"fmt"
	"log/slog"

	"github.com/hamba/avro/v2"
	"github.com/riferrei/srclient"
	"github.com/segmentio/kafka-go"
	"gitlab.education.tbank.ru/backend-academy-go-2025/homeworks/link-tracker/internal/domain"
)

type Consumer struct {
	reader   *kafka.Reader
	srClient *srclient.SchemaRegistryClient
	logger   *slog.Logger
	schemas  map[int]avro.Schema
}

func NewConsumer(brokers []string, topic, groupID, registryURL string, logger *slog.Logger) *Consumer {
	return &Consumer{
		reader: kafka.NewReader(kafka.ReaderConfig{
			Brokers: brokers,
			Topic:   topic,
			GroupID: groupID,
		}),
		srClient: srclient.CreateSchemaRegistryClient(registryURL),
		logger:   logger,
		schemas:  make(map[int]avro.Schema),
	}
}

func (c *Consumer) FetchUpdate(ctx context.Context) (domain.LinkUpdate, error) {
	msg, err := c.reader.ReadMessage(ctx)
	if err != nil {
		return domain.LinkUpdate{}, err
	}

	if len(msg.Value) < 5 || msg.Value[0] != 0 {
		return domain.LinkUpdate{}, fmt.Errorf("invalid message format: missing magic byte")
	}

	schemaID := int(binary.BigEndian.Uint32(msg.Value[1:5]))

	parsedSchema, err := c.getSchema(schemaID)
	if err != nil {
		return domain.LinkUpdate{}, fmt.Errorf("failed to get schema: %w", err)
	}
	
	var update domain.LinkUpdate
	err = avro.Unmarshal(parsedSchema, msg.Value[5:], &update)
	if err != nil {
		return domain.LinkUpdate{}, fmt.Errorf("failed to unmarshal avro: %w", err)
	}

	return update, nil
}

func (c *Consumer) getSchema(id int) (avro.Schema, error) {
	if s, ok := c.schemas[id]; ok {
		return s, nil
	}

	schema, err := c.srClient.GetSchema(id)
	if err != nil {
		return nil, err
	}

	parsed, err := avro.Parse(schema.Schema())
	if err != nil {
		return nil, err
	}

	c.schemas[id] = parsed
	return parsed, nil
}

func (c *Consumer) Close() error {
	return c.reader.Close()
}
