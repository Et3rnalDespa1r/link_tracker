package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"gitlab.education.tbank.ru/backend-academy-go-2025/homeworks/link-tracker/internal/application"
	"gitlab.education.tbank.ru/backend-academy-go-2025/homeworks/link-tracker/internal/infrastructure/config"
	"gitlab.education.tbank.ru/backend-academy-go-2025/homeworks/link-tracker/internal/infrastructure/db"
	"gitlab.education.tbank.ru/backend-academy-go-2025/homeworks/link-tracker/internal/infrastructure/http"
	"gitlab.education.tbank.ru/backend-academy-go-2025/homeworks/link-tracker/internal/infrastructure/repository/postgres"
	"gitlab.education.tbank.ru/backend-academy-go-2025/homeworks/link-tracker/internal/infrastructure/telegram"
)

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))

	cfg, err := config.Load()
	if err != nil {
		logger.Error("Не удалось загрузить конфиг", slog.Any("error", err))
		os.Exit(1)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	if err := db.RunMigrations(cfg.DatabaseDSN, logger); err != nil {
		logger.Error("Ошибка при запуске миграций", slog.Any("error", err))
		os.Exit(1)
	}

	pool, err := pgxpool.New(ctx, cfg.DatabaseDSN)
	if err != nil {
		logger.Error("Ошибка подключения к БД", slog.Any("error", err))
		os.Exit(1)
	}
	defer pool.Close()

	var repo application.LinkRepository
	if cfg.AccessType == "SQL" {
		repo = postgres.NewSqlLinkRepository(pool)
		logger.Info("Используется Raw SQL репозиторий")
	} else if cfg.AccessType == "ORM" {
		repo = postgres.NewBuilderLinkRepository(pool)
		logger.Info("Используется Query Builder (ORM) репозиторий")
	} else {
		logger.Error("Неизвестный тип доступа к БД", slog.String("ACCESS_TYPE", cfg.AccessType))
		os.Exit(1)
	}

	tgBot, err := telegram.NewBot(cfg.TelegramToken, cfg.BotTimeout, logger)
	if err != nil {
		logger.Error("Ошибка инициализации Telegram-клиента", slog.Any("error", err))
		os.Exit(1)
	}

	botService := application.NewBotService(repo, tgBot, logger)
	tgBot.SetService(botService)

	if err := tgBot.SetupCommands(); err != nil {
		logger.Error("Не удалось установить команды меню", slog.Any("error", err))
	}

	httpServer := http.NewBotServer(cfg.BotAddress, botService, logger)

	go func() {
		if err := httpServer.Start(); err != nil {
			logger.Error("Ошибка работы HTTP сервера", slog.Any("error", err))
		}
	}()

	go tgBot.Start(ctx)

	<-ctx.Done()

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := httpServer.Stop(shutdownCtx); err != nil {
		logger.Error("Ошибка при остановке HTTP сервера", slog.Any("error", err))
	}

	logger.Info("Приложение успешно остановлено")
}
