package main

import (
	"log/slog"
	"os"

	"gitlab.education.tbank.ru/backend-academy-go-2025/homeworks/link-tracker/internal/infrastructure/config"
	"gitlab.education.tbank.ru/backend-academy-go-2025/homeworks/link-tracker/internal/infrastructure/telegram"

	tgbotapi "github.com/go-telegram-bot-api/telegram-bot-api/v5"
)

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	slog.SetDefault(logger)

	cfg, err := config.LoadConfig()
	if err != nil {
		logger.Error("Не удалось загрузить конфиг", slog.String("error", err.Error()))
		os.Exit(1)
	}

	api, err := tgbotapi.NewBotAPI(cfg.TelegramToken)
	if err != nil {
		logger.Error("Ошибка авторизации бота", slog.String("error", err.Error()))
		os.Exit(1)
	}

	logger.Info("Бот успешно запущен", slog.String("account", api.Self.UserName))
	
	tgBot := telegram.NewBot(api, logger)

	if err := tgBot.SetupCommands(); err != nil {
		logger.Error("Не удалось установить команды меню", slog.String("error", err.Error()))
	}

	tgBot.Start()
}
