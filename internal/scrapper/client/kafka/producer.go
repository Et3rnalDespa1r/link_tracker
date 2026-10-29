package kafka

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"time"

	"github.com/hamba/avro/v2"
	"github.com/riferrei/srclient"
	"github.com/segmentio/kafka-go"
	"gitlab.education.tbank.ru/backend-academy-go-2025/homeworks/link-tracker/internal/scrapper/domain"
)

const linkUpdateSchema = `{
  "type": "record",
  "name": "LinkUpdate",
  "namespace": "ru.tbank.linktracker",
  "fields": [
    {"name": "id", "type": "long"},
    {"name": "url", "type": "string"},
    {"name": "description", "type": "string"},
    {"name": "tgChatIds", "type": {"type": "array", "items": "long"}}
  ]
}`

type Producer struct {
	jsonMode     bool
	writer       *kafka.Writer
	srClient     *srclient.SchemaRegistryClient
	schemaID     int
	parsedSchema avro.Schema
}

func NewProducer(brokers []string, topic string, schemaRegistryURL string) (*Producer, error) {
	w := &kafka.Writer{
		Addr:         kafka.TCP(brokers...),
		Topic:        topic,
		Balancer:     &kafka.Hash{},
		RequiredAcks: kafka.RequireAll,
		WriteTimeout: 15 * time.Second,
	}

	srClient := srclient.CreateSchemaRegistryClient(schemaRegistryURL)

	subject := topic + "-value"
	schema, err := srClient.GetLatestSchema(subject)
	if err != nil {
		schema, err = srClient.CreateSchema(subject, linkUpdateSchema, srclient.Avro)
		if err != nil {
			return nil, fmt.Errorf("failed to register avro schema: %w", err)
		}
	}

	parsed, err := avro.Parse(schema.Schema())
	if err != nil {
		return nil, fmt.Errorf("ошибка парсинга схемы avro: %w", err)
	}

	return &Producer{
		writer:       w,
		srClient:     srClient,
		schemaID:     schema.ID(),
		parsedSchema: parsed,
	}, nil
}

func (p *Producer) SendUpdate(ctx context.Context, update domain.LinkUpdate) error {
	if p.jsonMode {
		payload, err := json.Marshal(update)
		if err != nil {
			return err
		}
		return p.writer.WriteMessages(ctx, kafka.Message{Key: []byte(fmt.Sprint(update.ID)), Value: payload})
	}
	avroBytes, err := avro.Marshal(p.parsedSchema, update)
	if err != nil {
		return fmt.Errorf("ошибка сериализации avro: %w", err)
	}

	schemaIDBytes := make([]byte, 4)
	binary.BigEndian.PutUint32(schemaIDBytes, uint32(p.schemaID))

	var recordValue []byte
	recordValue = append(recordValue, byte(0))
	recordValue = append(recordValue, schemaIDBytes...)
	recordValue = append(recordValue, avroBytes...)

	msg := kafka.Message{
		Key:   []byte(fmt.Sprint(update.ID)),
		Value: recordValue,
	}

	if err := p.writer.WriteMessages(ctx, msg); err != nil {
		return fmt.Errorf("ошибка отправки в kafka: %w", err)
	}

	return nil
}

func (p *Producer) Close() error {
	return p.writer.Close()
}

func NewJSONProducer(brokers []string, topic string) *Producer {
	return &Producer{jsonMode: true, writer: &kafka.Writer{Addr: kafka.TCP(brokers...), Topic: topic, Balancer: &kafka.Hash{}, RequiredAcks: kafka.RequireAll, WriteTimeout: 10 * time.Second}}
}
