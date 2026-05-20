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
	app     *application.BotService
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

func (b *Bot) SetService(app *application.BotService) {
	b.app = app
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

func (b *Bot) Start(ctx context.Context) error {
	u := tgbotapi.NewUpdate(updateOffset)
	u.Timeout = b.timeout

	updates := b.api.GetUpdatesChan(u)
	b.logger.Info("Начинаю прослушивание обновлений...")

	for {
		select {
		case <-ctx.Done():
			b.logger.Info("Завершение работы Telegram-клиента...")
			return nil
		case update, ok := <-updates:
			if !ok {
				return fmt.Errorf("updates channel closed unexpectedly")
			}
			if update.Message == nil {
				continue
			}
			b.handleMessage(ctx, update.Message)
		}
	}
}

func (b *Bot) handleMessage(ctx context.Context, msg *tgbotapi.Message) {
	b.logger.Info("Получено сообщение",
		slog.String("user", msg.From.UserName),
		slog.String("text", msg.Text),
		slog.Int("message_id", msg.MessageID),
	)

	if b.app == nil {
		return
	}

	var replyText string

	if msg.IsCommand() {
		replyText = b.app.HandleCommand(ctx, msg.Chat.ID, msg.Command(), msg.CommandArguments())
	} else {
		replyText = b.app.HandleText(ctx, msg.Chat.ID, msg.Text)
	}

	if err := b.SendMessage(msg.Chat.ID, replyText); err != nil {
		b.logger.Error("Ошибка при отправке ответа", "err", err)
	}
}

func (b *Bot) SendMessage(chatID int64, text string) error {
	reply := tgbotapi.NewMessage(chatID, text)
	_, err := b.api.Send(reply)
	return err
}

func (b *Bot) SendNotification(ctx context.Context, chatID int64, text string) error {
	return b.SendMessage(chatID, text)
}
