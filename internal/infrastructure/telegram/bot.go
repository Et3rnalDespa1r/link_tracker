package telegram

import (
	"context"
	"fmt"
	"log/slog"

	tgbotapi "github.com/go-telegram-bot-api/telegram-bot-api/v5"
	"gitlab.education.tbank.ru/backend-academy-go-2025/homeworks/link-tracker/internal/application"
)

const updateOffset = 0

type BotAPI interface {
	Request(c tgbotapi.Chattable) (*tgbotapi.APIResponse, error)
	GetUpdatesChan(config tgbotapi.UpdateConfig) tgbotapi.UpdatesChannel
	Send(c tgbotapi.Chattable) (tgbotapi.Message, error)
}

type Bot struct {
	api     BotAPI
	logger  *slog.Logger
	timeout int
}

func NewBot(token string, timeout int, logger *slog.Logger) (*Bot, error) {
	api, err := tgbotapi.NewBotAPI(token)
	if err != nil {
		return nil, fmt.Errorf("failed to create telegram bot api: %w", err)
	}

	return &Bot{
		api:     api,
		logger:  logger,
		timeout: timeout,
	}, nil
}

func (b *Bot) SetupCommands() error {
	commands := tgbotapi.NewSetMyCommands(
		tgbotapi.BotCommand{Command: application.CmdStart, Description: application.DescStart},
		tgbotapi.BotCommand{Command: application.CmdHelp, Description: application.DescHelp},
	)
	resp, err := b.api.Request(commands)
	if err != nil {
		return fmt.Errorf("failed to set bot commands: %w", err)
	}
	if !resp.Ok {
		return fmt.Errorf("failed to set bot commands, API response not OK: %s", resp.Description)
	}

	return nil
}

func (b *Bot) Start(ctx context.Context) {
	u := tgbotapi.NewUpdate(updateOffset)
	u.Timeout = b.timeout

	updates := b.api.GetUpdatesChan(u)

	b.logger.Info("Начинаю прослушивание обновлений...")

	for {
		select {
		case <-ctx.Done():
			b.logger.Info("Завершение работы бота (Graceful Shutdown)...")
			return
		case update, ok := <-updates:
			if !ok {
				return
			}
			if update.Message == nil || !update.Message.IsCommand() {
				continue
			}
			b.handleMessage(update.Message)
		}
	}
}

func (b *Bot) handleMessage(msg *tgbotapi.Message) {
	b.logger.Info("Получена команда",
		slog.String("user", msg.From.UserName),
		slog.String("command", msg.Command()),
		slog.Int("message_id", msg.MessageID),
	)

	replyText := application.RouteCommand(msg.Command())

	b.sendMessage(msg.Chat.ID, replyText)
}

func (b *Bot) sendMessage(chatID int64, text string) {
	reply := tgbotapi.NewMessage(chatID, text)
	if _, err := b.api.Send(reply); err != nil {
		b.logger.Error("Ошибка при отправке сообщения", slog.Any("error", err))
	}
}
