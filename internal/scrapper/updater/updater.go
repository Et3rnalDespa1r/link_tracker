package updater

import (
	"context"
	"log/slog"

	"gitlab.education.tbank.ru/backend-academy-go-2025/homeworks/link-tracker/internal/scrapper/client"
	"gitlab.education.tbank.ru/backend-academy-go-2025/homeworks/link-tracker/internal/scrapper/domain"
)

type Updater struct {
	botClient *client.BotClient
	logger    *slog.Logger
}

func NewUpdater(botClient *client.BotClient, logger *slog.Logger) *Updater {
	return &Updater{
		botClient: botClient,
		logger:    logger,
	}
}

func (u *Updater) ProcessUpdates(ctx context.Context) {
	u.logger.Info("Начата проверка обновлений по расписанию...")

	update := domain.LinkUpdate{
		ID:          1,
		URL:         "https://github.com/test/repo",
		Description: "Обнаружено обновление в репозитории (Заглушка из Scrapper)",
		TgChatIDs:   []int64{12345},
	}

	if err := u.botClient.SendUpdate(ctx, update); err != nil {
		u.logger.Error("Ошибка при отправке обновления боту", slog.Any("error", err))
	} else {
		u.logger.Info("Уведомление об обновлении успешно отправлено боту")
	}
}
