package main

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/prometheus/client_golang/prometheus/push"
	"gitlab.education.tbank.ru/backend-academy-go-2025/homeworks/link-tracker/internal/application"
	"gitlab.education.tbank.ru/backend-academy-go-2025/homeworks/link-tracker/internal/config"
	infraHttp "gitlab.education.tbank.ru/backend-academy-go-2025/homeworks/link-tracker/internal/infrastructure/http"
	"gitlab.education.tbank.ru/backend-academy-go-2025/homeworks/link-tracker/internal/infrastructure/kafka"
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

	go func() {
		metricsMux := http.NewServeMux()
		metricsMux.Handle("/metrics", promhttp.Handler())
		logger.Info("Запуск сервера метрик для Bot", "port", "8011")
		if err := http.ListenAndServe(":8011", metricsMux); err != nil && err != http.ErrServerClosed {
			logger.Error("Ошибка сервера метрик Bot", "error", err)
		}
	}()

	go func() {
		ticker := time.NewTicker(5 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				if cfg.PushgatewayURL == "" {
					continue
				}
				err := push.New(cfg.PushgatewayURL, "bot").
					Gatherer(prometheus.DefaultGatherer).
					Push()
				if err != nil {
					logger.Error("Ошибка отправки метрик в Pushgateway", "error", err)
				}
			}
		}
	}()

	repo := infraHttp.NewScrapperRepository(cfg.ScrapperURL, cfg.HTTPTimeout)

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

	{ // Kafka also receives fallback notifications in HTTP mode.
		dlqTopic := cfg.AgentProcessedTopic + ".dlq"

		kafkaConsumer := kafka.NewConsumer(
			cfg.KafkaBrokers,
			cfg.AgentProcessedTopic,
			dlqTopic,
			cfg.KafkaGroupID,
			logger,
			cfg.KafkaRetries,
			tgBot,
			cfg.SchemaRegistryURL,
		)

		go func() {
			defer kafkaConsumer.Close()
			kafkaConsumer.Start(ctx)
		}()

		logger.Info("Бот слушает обновления через Kafka (Avro)", slog.Any("brokers", cfg.KafkaBrokers))
	}

	httpServer := infraHttp.NewBotServer(cfg.BotAddress, botService, logger)

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
