package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"gitlab.education.tbank.ru/backend-academy-go-2025/homeworks/link-tracker/internal/application"
	"gitlab.education.tbank.ru/backend-academy-go-2025/homeworks/link-tracker/internal/config"
	"gitlab.education.tbank.ru/backend-academy-go-2025/homeworks/link-tracker/internal/infrastructure/db"
	"gitlab.education.tbank.ru/backend-academy-go-2025/homeworks/link-tracker/internal/infrastructure/http"
	"gitlab.education.tbank.ru/backend-academy-go-2025/homeworks/link-tracker/internal/infrastructure/kafka"
	"gitlab.education.tbank.ru/backend-academy-go-2025/homeworks/link-tracker/internal/infrastructure/repository/postgres"
	"gitlab.education.tbank.ru/backend-academy-go-2025/homeworks/link-tracker/internal/infrastructure/telegram"
)

const shutdownTimeout = 5 * time.Second

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))

	cfg, err := config.Load()
	if err != nil {
		logger.Error("Не удалось загрузить конфиг", "error", err)
		os.Exit(1)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	if err := db.RunMigrations(cfg.DatabaseDSN(), logger); err != nil {
		logger.Error("Ошибка при запуске миграций", slog.Any("error", err))
		os.Exit(1)
	}

	pool, err := pgxpool.New(ctx, cfg.DatabaseDSN())
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
		logger.Error("Ошибка инициализации Telegram-клиента", "error", err)
		os.Exit(1)
	}

	botService := application.NewBotService(repo, tgBot, logger)
	tgBot.SetService(botService)

	if err := tgBot.SetupCommands(); err != nil {
		logger.Error("Не удалось установить команды меню", "error", err)
	}

	if cfg.UseKafka {
		groupID := "link-tracker-bot-group"
		schemaRegistryURL := "http://localhost:8081"

		kafkaConsumer := kafka.NewConsumer(
			cfg.KafkaBrokers,
			cfg.KafkaTopic,
			groupID,
			schemaRegistryURL,
			logger,
		)

		go func() {
			defer kafkaConsumer.Close()
			for {
				update, err := kafkaConsumer.FetchUpdate(ctx)
				if err != nil {
					if ctx.Err() != nil {
						return
					}
					logger.Error("Ошибка получения обновления", "err", err)
					continue
				}

				logger.Info("Получено Avro обновление!", "url", update.URL)

				if err := botService.Notify(ctx, update); err != nil {
					logger.Error("Ошибка при отправке обновления пользователям", "err", err)
				}
			}
		}()

		logger.Info("Бот слушает обновления через Kafka (Avro)", slog.Any("brokers", cfg.KafkaBrokers))
	} else {
		logger.Info("Бот готов принимать обновления по HTTP")
	}

	httpServer := http.NewBotServer(cfg.BotAddress, botService, logger)

	errCh := make(chan error, 2)

	go func() {
		if err := httpServer.Start(); err != nil {
			errCh <- fmt.Errorf("ошибка HTTP сервера: %w", err)
		}
	}()

	go func() {
		if err := tgBot.Start(ctx); err != nil {
			errCh <- fmt.Errorf("ошибка Telegram бота: %w", err)
		}
	}()

	select {
	case <-ctx.Done():
		logger.Info("Остановка приложения по сигналу...")
	case err := <-errCh:
		logger.Error("Аварийная остановка приложения", "error", err)
	}

	shutdownCtx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
	defer cancel()

	if err := httpServer.Stop(shutdownCtx); err != nil {
		logger.Error("Ошибка при остановке HTTP сервера", "error", err)
	}

	logger.Info("Приложение успешно остановлено")
}
