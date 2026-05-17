package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/go-co-op/gocron"
	"github.com/jackc/pgx/v5/pgxpool"
	"gitlab.education.tbank.ru/backend-academy-go-2025/homeworks/link-tracker/internal/config"
	"gitlab.education.tbank.ru/backend-academy-go-2025/homeworks/link-tracker/internal/infrastructure/cache"
	"gitlab.education.tbank.ru/backend-academy-go-2025/homeworks/link-tracker/internal/infrastructure/repository/postgres"
	"gitlab.education.tbank.ru/backend-academy-go-2025/homeworks/link-tracker/internal/scrapper/client"
	"gitlab.education.tbank.ru/backend-academy-go-2025/homeworks/link-tracker/internal/scrapper/client/kafka"
	"gitlab.education.tbank.ru/backend-academy-go-2025/homeworks/link-tracker/internal/scrapper/http"
	"gitlab.education.tbank.ru/backend-academy-go-2025/homeworks/link-tracker/internal/scrapper/updater"
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

	pool, err := pgxpool.New(ctx, cfg.DatabaseDSN())
	if err != nil {
		logger.Error("Ошибка подключения к БД", "error", err)
		os.Exit(1)
	}
	defer pool.Close()

	repo := postgres.NewSqlLinkRepository(pool)
	var sender updater.UpdateSender
	valkeyClient, err := cache.NewValkeyClient(cfg.ValkeyAddresses, logger)
	if err != nil {
		logger.Error("Не удалось инициализировать Valkey", "error", err)
	} else {
		defer valkeyClient.Close()
	}
	if cfg.UseKafka {
		schemaRegistryURL := "http://localhost:8081"
		kafkaProducer, err := kafka.NewProducer(cfg.KafkaBrokers, cfg.KafkaTopic, schemaRegistryURL)
		if err != nil {
			logger.Error("Не удалось создать Avro Producer", "error", err)
			os.Exit(1)
		}

		defer func() {
			if err := kafkaProducer.Close(); err != nil {
				logger.Error("Ошибка закрытия Kafka продюсера", "error", err)
			}
		}()
		sender = kafkaProducer

		outboxRepo := postgres.NewOutboxRepository(pool)
		outboxProcessor := kafka.NewOutboxProcessor(outboxRepo, kafkaProducer, logger)
		go outboxProcessor.Run(ctx)

		logger.Info("Скраппер запущен. Уведомления отправляются через Kafka (Outbox + Avro)", slog.Any("brokers", cfg.KafkaBrokers))
	} else {
		botClient := client.NewBotClient(cfg.BotURL)
		sender = botClient
		logger.Info("Скраппер запущен. Уведомления отправляются через HTTP", slog.String("url", cfg.BotURL))
	}

	githubClient := client.NewGithubClient("https://api.github.com", cfg.GithubToken)
	soClient := client.NewStackOverflowClient("https://api.stackexchange.com/2.3")

	updateSvc := updater.NewUpdater(sender, repo, cfg, logger, githubClient, soClient)

	scheduler := gocron.NewScheduler(time.UTC)

	_, err = scheduler.Every(cfg.CheckInterval).Do(func() {
		updateSvc.ProcessUpdates(ctx)
	})
	if err != nil {
		logger.Error("Ошибка настройки планировщика", "error", err)
		os.Exit(1)
	}
	scheduler.StartAsync()
	respCache := cache.NewResponseCache(valkeyClient, cfg.ValkeyCacheTTL)
	httpServer := http.NewScrapperServer(cfg.ServerAddress, logger, respCache, repo)

	errCh := make(chan error, 1)

	go func() {
		if err := httpServer.Start(); err != nil {
			errCh <- fmt.Errorf("ошибка HTTP сервера: %w", err)
		}
	}()

	select {
	case <-ctx.Done():
		logger.Info("Остановка скраппера по сигналу...")
	case err := <-errCh:
		logger.Error("Аварийная остановка скраппера", "error", err)
	}

	scheduler.Stop()

	shutdownCtx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
	defer cancel()

	if err := httpServer.Stop(shutdownCtx); err != nil {
		logger.Error("Ошибка при остановке HTTP сервера", "error", err)
	}

	logger.Info("Приложение успешно остановлено")
}
