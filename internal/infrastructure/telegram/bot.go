package telegram

import (
	"log/slog"

	tgbotapi "github.com/go-telegram-bot-api/telegram-bot-api/v5"
)

type Bot struct {
	api    *tgbotapi.BotAPI
	logger *slog.Logger
}

func NewBot(api *tgbotapi.BotAPI, logger *slog.Logger) *Bot {
	return &Bot{
		api:    api,
		logger: logger,
	}
}

func (b *Bot) SetupCommands() error {
	commands := tgbotapi.NewSetMyCommands(
		tgbotapi.BotCommand{Command: "start", Description: "Начать работу с ботом"},
		tgbotapi.BotCommand{Command: "help", Description: "Показать список доступных команд"},
	)
	_, err := b.api.Request(commands)
	return err
}

func (b *Bot) Start() {
	u := tgbotapi.NewUpdate(0)
	u.Timeout = 60
	updates := b.api.GetUpdatesChan(u)

	b.logger.Info("Начинаю прослушивание обновлений...")

	for update := range updates {
		if update.Message == nil || !update.Message.IsCommand() {
			continue
		}

		b.handleMessage(update.Message)
	}
}

func RouteCommand(command string) string {
	switch command {
	case "start":
		return "Добро пожаловать в LinkTracker! Используйте /help, чтобы посмотреть доступные команды."
	case "help":
		return "Доступные команды:\n/start - Начало работы\n/help - Список команд"
	default:
		return "Неизвестная команда. Воспользуйтесь /help, чтобы посмотреть список доступных команд."
	}
}

func (b *Bot) handleMessage(msg *tgbotapi.Message) {
	b.logger.Info("Получена команда",
		slog.String("user", msg.From.UserName),
		slog.String("command", msg.Command()),
	)

	replyText := RouteCommand(msg.Command())

	b.sendMessage(msg.Chat.ID, replyText)
}

func (b *Bot) sendMessage(chatID int64, text string) {
	reply := tgbotapi.NewMessage(chatID, text)
	if _, err := b.api.Send(reply); err != nil {
		b.logger.Error("Ошибка при отправке сообщения", slog.String("error", err.Error()))
	}
}
