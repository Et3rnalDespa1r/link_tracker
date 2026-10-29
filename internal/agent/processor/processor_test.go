package processor

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestProcessor_Priority(t *testing.T) {
	proc := NewProcessor(nil, nil, 0, 100, "", []string{"critical"}, []string{"typo"})

	high := proc.ProcessUpdate(RawUpdate{Description: "fixed a critical bug"})
	assert.NotNil(t, high)
	assert.Equal(t, PriorityHigh, high.Priority)

	med := proc.ProcessUpdate(RawUpdate{Description: "some normal update"})
	assert.NotNil(t, med)
	assert.Equal(t, PriorityMedium, med.Priority)

	low := proc.ProcessUpdate(RawUpdate{Description: "fixed a typo in readme"})
	assert.NotNil(t, low)
	assert.Equal(t, PriorityLow, low.Priority)
}

func TestGrouper(t *testing.T) {
	g := NewGrouper(50 * time.Millisecond)
	g.Add(ProcessedUpdate{ID: 1, Description: "first", TgChatIDs: []int64{123}, Priority: PriorityLow})
	g.Add(ProcessedUpdate{ID: 2, Description: "second", TgChatIDs: []int64{123}, Priority: PriorityHigh})

	res := <-g.Out()
	assert.Len(t, res, 1, "Должно быть сгруппировано в одно сообщение")
	assert.Equal(t, PriorityHigh, res[0].Priority, "Приоритет должен быть максимальным")
	assert.Contains(t, res[0].Description, "1. first")
	assert.Contains(t, res[0].Description, "2. second")

	g2 := NewGrouper(50 * time.Millisecond)
	g2.Add(ProcessedUpdate{ID: 3, Description: "single", TgChatIDs: []int64{456}, Priority: PriorityMedium})

	res2 := <-g2.Out()
	assert.Len(t, res2, 1)
	assert.Equal(t, "single", res2[0].Description, "Одиночное сообщение не должно меняться")
	assert.Equal(t, PriorityMedium, res2[0].Priority)
}

func TestProcessor_Filtering(t *testing.T) {
	proc := NewProcessor([]string{"spam"}, []string{"bad_author"}, 10, 100, "", nil, nil)

	assert.Nil(t, proc.ProcessUpdate(RawUpdate{Description: "short"}))
	assert.Nil(t, proc.ProcessUpdate(RawUpdate{Author: "bad_author", Description: "long enough description"}))
	assert.Nil(t, proc.ProcessUpdate(RawUpdate{Description: "this is spam message long enough"}))
	assert.NotNil(t, proc.ProcessUpdate(RawUpdate{Description: "this is a valid message long enough"}))
}

func TestGrouperOverlappingRecipients(t *testing.T) {
	g := NewGrouper(time.Hour)
	g.Add(ProcessedUpdate{ID: 1, Description: "first", TgChatIDs: []int64{1, 2}, Priority: PriorityLow})
	g.Add(ProcessedUpdate{ID: 2, Description: "second", TgChatIDs: []int64{2, 3}, Priority: PriorityHigh})
	g.flushTimer.Stop()
	g.flush()
	result := <-g.Out()
	assert.Len(t, result, 3)
	for _, update := range result {
		assert.Len(t, update.TgChatIDs, 1)
		if update.TgChatIDs[0] == 2 {
			assert.Contains(t, update.Description, "1. first")
			assert.Contains(t, update.Description, "2. second")
			assert.Equal(t, PriorityHigh, update.Priority)
		}
	}
}
