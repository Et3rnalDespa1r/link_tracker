package main

import (
	"bufio"
	"context"
	"encoding/json"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/segmentio/kafka-go"
	"github.com/stretchr/testify/require"
	tckafka "github.com/testcontainers/testcontainers-go/modules/kafka"
	"gitlab.education.tbank.ru/backend-academy-go-2025/homeworks/link-tracker/internal/agent/processor"
)

func TestAgentRecoveryChild(t *testing.T) {
	if os.Getenv("LINKTRACKER_RECOVERY_CHILD") != "1" {
		return
	}
	main()
	os.Exit(0)
}

func TestAgentProcessRestartDrainsQueuedUpdates(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	container, err := tckafka.RunContainer(ctx)
	require.NoError(t, err)
	defer container.Terminate(context.Background())
	brokers, err := container.Brokers(ctx)
	require.NoError(t, err)
	raw, processed := "recovery.raw-updates", "recovery.processed-updates"
	connection, err := kafka.DialContext(ctx, "tcp", brokers[0])
	require.NoError(t, err)
	require.NoError(t, connection.CreateTopics(kafka.TopicConfig{Topic: raw, NumPartitions: 1, ReplicationFactor: 1}, kafka.TopicConfig{Topic: processed, NumPartitions: 1, ReplicationFactor: 1}))
	connection.Close()
	start := func() *exec.Cmd {
		command := exec.Command(os.Args[0], "-test.run=^TestAgentRecoveryChild$")
		command.Env = append(os.Environ(), "LINKTRACKER_RECOVERY_CHILD=1", "APP_TELEGRAM_TOKEN=test-token", "DB_PASSWORD=test-password", "AGENT_HF_TOKEN=", "KAFKA_BROKERS="+strings.Join(brokers, ","), "AGENT_RAW_TOPIC="+raw, "AGENT_PROCESSED_TOPIC="+processed, "AGENT_GROUP_ID=recovery-agent", "AGENT_GROUP_WINDOW=500ms", "AGENT_MIN_LENGTH=0")
		output, err := command.StdoutPipe()
		require.NoError(t, err)
		command.Stderr = os.Stderr
		require.NoError(t, command.Start())
		ready := make(chan bool, 1)
		go func() {
			scanner := bufio.NewScanner(output)
			announced := false
			for scanner.Scan() {
				if !announced && strings.Contains(scanner.Text(), "AI Agent started") {
					ready <- true
					announced = true
				}
			}
			if !announced {
				ready <- false
			}
		}()
		select {
		case ok := <-ready:
			require.True(t, ok, "agent exited before readiness")
		case <-time.After(20 * time.Second):
			command.Process.Kill()
			command.Wait()
			t.Fatal("agent readiness timeout")
		}
		return command
	}
	first := start()
	require.NoError(t, first.Process.Kill())
	require.Error(t, first.Wait())
	t.Log("Agent is stopped; publishing updates to Kafka backlog")
	writer := &kafka.Writer{Addr: kafka.TCP(brokers...), Topic: raw, RequiredAcks: kafka.RequireAll}
	defer writer.Close()
	payload, err := json.Marshal(processor.RawUpdate{ID: 42, URL: "https://github.com/test/recovery", Author: "alice", Description: "critical recovery notification", TgChatIDs: []int64{123}})
	require.NoError(t, err)
	require.NoError(t, writer.WriteMessages(ctx, kafka.Message{Value: payload}))
	second := start()
	defer func() { second.Process.Signal(os.Interrupt); second.Wait() }()
	reader := kafka.NewReader(kafka.ReaderConfig{Brokers: brokers, Topic: processed, Partition: 0, StartOffset: kafka.FirstOffset, MaxWait: time.Second})
	defer reader.Close()
	timeout, endRead := context.WithTimeout(ctx, 40*time.Second)
	defer endRead()
	message, err := reader.ReadMessage(timeout)
	require.NoError(t, err)
	var update processor.ProcessedUpdate
	require.NoError(t, json.Unmarshal(message.Value, &update))
	require.Equal(t, int64(42), update.ID)
	require.Equal(t, processor.PriorityHigh, update.Priority)
	require.Equal(t, []int64{123}, update.TgChatIDs)
	t.Log("Restarted agent delivered the queued update with HIGH priority")
}
