package kafka

import (
	"context"
	"encoding/json"
	"log/slog"
	"time"

	"gitlab.education.tbank.ru/backend-academy-go-2025/homeworks/link-tracker/internal/infrastructure/repository/postgres"
	scrapDomain "gitlab.education.tbank.ru/backend-academy-go-2025/homeworks/link-tracker/internal/scrapper/domain"
	"gitlab.education.tbank.ru/backend-academy-go-2025/homeworks/link-tracker/internal/scrapper/updater"
)

type OutboxProcessor struct {
	repo   *postgres.OutboxRepository
	sender updater.UpdateSender
	logger *slog.Logger
}

func NewOutboxProcessor(repo *postgres.OutboxRepository, sender updater.UpdateSender, logger *slog.Logger) *OutboxProcessor {
	return &OutboxProcessor{repo: repo, sender: sender, logger: logger}
}

func (p *OutboxProcessor) Run(ctx context.Context) {
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			p.process(ctx)
		}
	}
}

func (p *OutboxProcessor) process(ctx context.Context) {
	pending, err := p.repo.FetchPending(ctx)
	if err != nil {
		p.logger.Error("Outbox: ошибка получения записей", "err", err)
		return
	}
	if len(pending) == 0 {
		return
	}

	for id, payload := range pending {
		var update scrapDomain.LinkUpdate
		if err := json.Unmarshal(payload, &update); err != nil {
			p.logger.Error("Outbox: ошибка десериализации JSON", "id", id, "err", err)
			continue
		}

		if err := p.sender.SendUpdate(ctx, update); err != nil {
			p.logger.Error("Outbox: НЕ УДАЛОСЬ отправить в Kafka", "id", id, "err", err)
			continue
		}

		if err := p.repo.MarkProcessed(ctx, id); err != nil {
			p.logger.Error("Outbox: ошибка обновления статуса в БД", "id", id, "err", err)
		} else {
			p.logger.Info("Outbox: сообщение успешно улетело в Kafka", "id", id)
		}
	}
}
