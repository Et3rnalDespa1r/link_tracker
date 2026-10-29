package kafka_test

import (
	"context"
	"encoding/json"
	"github.com/moby/moby/api/types/container"
	"github.com/moby/moby/api/types/network"
	"log/slog"
	"net"
	"net/netip"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	broker "github.com/segmentio/kafka-go"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	tckafka "github.com/testcontainers/testcontainers-go/modules/kafka"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"
	"gitlab.education.tbank.ru/backend-academy-go-2025/homeworks/link-tracker/internal/domain"
	"gitlab.education.tbank.ru/backend-academy-go-2025/homeworks/link-tracker/internal/infrastructure/repository/postgres"
	transport "gitlab.education.tbank.ru/backend-academy-go-2025/homeworks/link-tracker/internal/scrapper/client/kafka"
	events "gitlab.education.tbank.ru/backend-academy-go-2025/homeworks/link-tracker/internal/scrapper/domain"
)

func TestRecoveryRealDatabaseAndKafkaRestart(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	pg, err := tcpostgres.RunContainer(ctx, testcontainers.WithImage("postgres:15-alpine"), tcpostgres.WithDatabase("recovery"), stableBinding(t, "5432/tcp"), tcpostgres.WithUsername("test"), tcpostgres.WithPassword("test"), testcontainers.WithWaitStrategy(wait.ForLog("database system is ready to accept connections").WithOccurrence(2).WithStartupTimeout(time.Minute)))
	require.NoError(t, err)
	defer pg.Terminate(context.Background())
	dsn, err := pg.ConnectionString(ctx, "sslmode=disable")
	require.NoError(t, err)
	pool, err := pgxpool.New(ctx, dsn)
	require.NoError(t, err)
	defer pool.Close()
	schema, err := os.ReadFile("../../../../migrations/000001_init.up.sql")
	require.NoError(t, err)
	_, err = pool.Exec(ctx, string(schema))
	require.NoError(t, err)
	repo := postgres.NewSqlLinkRepository(pool)
	require.NoError(t, repo.AddLink(ctx, 123, domain.Link{URL: "https://github.com/test/recovery", Tags: []string{"recovery"}}))
	links, err := repo.GetLinks(ctx, 123)
	require.NoError(t, err)
	require.Len(t, links, 1)
	update := events.LinkUpdate{ID: links[0].ID, URL: links[0].URL, Description: "recovery notification", Author: "recovery-test", TgChatIDs: []int64{123}}
	require.NoError(t, repo.UpdateLinkAndSaveOutbox(ctx, domain.Link{ID: update.ID, LastCheckTime: time.Now(), UpdatedAt: time.Now()}, update))

	t.Log("Stopping PostgreSQL: requests must fail, then stored subscriptions and Outbox must survive restart")
	require.NoError(t, pg.Stop(ctx, nil))
	short, stop := context.WithTimeout(ctx, time.Second)
	_, err = repo.GetLinks(short, 123)
	stop()
	require.Error(t, err)
	require.NoError(t, pg.Start(ctx))
	require.Eventually(t, func() bool {
		attempt, c := context.WithTimeout(ctx, time.Second)
		defer c()
		return pool.Ping(attempt) == nil
	}, 30*time.Second, 250*time.Millisecond)
	links, err = repo.GetLinks(ctx, 123)
	require.NoError(t, err)
	require.Len(t, links, 1)
	require.Equal(t, []string{"recovery"}, links[0].Tags)

	kafka, err := tckafka.RunContainer(ctx, stableBinding(t, "9093/tcp"))
	require.NoError(t, err)
	defer kafka.Terminate(context.Background())
	brokers, err := kafka.Brokers(ctx)
	require.NoError(t, err)
	topic := "recovery.raw-updates"
	conn, err := broker.DialContext(ctx, "tcp", brokers[0])
	require.NoError(t, err)
	require.NoError(t, conn.CreateTopics(broker.TopicConfig{Topic: topic, NumPartitions: 1, ReplicationFactor: 1}))
	conn.Close()
	producer := transport.NewJSONProducer(brokers, topic)
	defer producer.Close()
	outbox := transport.NewOutboxProcessor(postgres.NewOutboxRepository(pool), producer, slog.Default())
	t.Log("Stopping Kafka: Outbox must remain PENDING after failed delivery")
	require.NoError(t, kafka.Stop(ctx, nil))
	failureCtx, endFailure := context.WithTimeout(ctx, 4*time.Second)
	done := make(chan struct{})
	go func() { defer close(done); outbox.Run(failureCtx) }()
	<-failureCtx.Done()
	endFailure()
	<-done
	var status string
	require.NoError(t, pool.QueryRow(ctx, "SELECT status FROM link_updates_outbox").Scan(&status))
	require.Equal(t, "PENDING", status)
	t.Log("Restarting Kafka: the existing processor must deliver the retained event")
	require.NoError(t, kafka.Start(ctx))
	recoveryCtx, endRecovery := context.WithCancel(ctx)
	recovered := make(chan struct{})
	go func() { defer close(recovered); outbox.Run(recoveryCtx) }()
	defer func() { endRecovery(); <-recovered }()
	reader := broker.NewReader(broker.ReaderConfig{Brokers: brokers, Topic: topic, Partition: 0, StartOffset: broker.FirstOffset, MaxWait: time.Second})
	defer reader.Close()
	readCtx, endRead := context.WithTimeout(ctx, 45*time.Second)
	defer endRead()
	var message broker.Message
	for {
		message, err = reader.ReadMessage(readCtx)
		if err == nil || readCtx.Err() != nil {
			break
		}
	}
	require.NoError(t, err)
	var received events.LinkUpdate
	require.NoError(t, json.Unmarshal(message.Value, &received))
	require.Equal(t, update, received)
	require.Eventually(t, func() bool {
		return pool.QueryRow(ctx, "SELECT status FROM link_updates_outbox").Scan(&status) == nil && status == "PROCESSED"
	}, 15*time.Second, 250*time.Millisecond)
}

func stableBinding(t *testing.T, port string) testcontainers.CustomizeRequestOption {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	hostPort := strconv.Itoa(listener.Addr().(*net.TCPAddr).Port)
	require.NoError(t, listener.Close())
	return testcontainers.WithHostConfigModifier(func(config *container.HostConfig) {
		if config.PortBindings == nil {
			config.PortBindings = network.PortMap{}
		}
		config.PortBindings[network.MustParsePort(port)] = []network.PortBinding{{HostIP: netip.MustParseAddr("127.0.0.1"), HostPort: hostPort}}
	})
}
