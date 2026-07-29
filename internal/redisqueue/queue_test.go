package redisqueue

import (
	"bytes"
	"encoding/json"
	"strconv"
	"strings"
	"testing"
	"time"
)

func TestEnqueueBroadcastsToUsageSubscribersAndSkipsQueue(t *testing.T) {
	withEnabledQueue(t, func() {
		first, unsubscribeFirst := SubscribeUsage()
		defer unsubscribeFirst()
		second, unsubscribeSecond := SubscribeUsage()
		defer unsubscribeSecond()

		requireUsageSubscriberPayload(t, first, usageSupportRefreshPayload)
		requireUsageSubscriberPayload(t, second, usageSupportRefreshPayload)

		Enqueue([]byte("usage-record"))

		requireUsageSubscriberPayload(t, first, "usage-record")
		requireUsageSubscriberPayload(t, second, "usage-record")

		if items := PopOldest(1); len(items) != 0 {
			t.Fatalf("PopOldest() items = %q, want empty after subscriber broadcast", items)
		}

		unsubscribeFirst()
		unsubscribeSecond()

		Enqueue([]byte("queued-record"))
		items := PopOldest(1)
		if len(items) != 1 || string(items[0]) != "queued-record" {
			t.Fatalf("PopOldest() items = %q, want queued record after unsubscribe", items)
		}
	})
}

func TestEnqueueRejectsOversizedPayloadBeforeSubscriberBroadcast(t *testing.T) {
	withEnabledQueue(t, func() {
		subscriber, unsubscribe := SubscribeUsage()
		defer unsubscribe()
		requireUsageSubscriberPayload(t, subscriber, usageSupportRefreshPayload)

		atLimitJSON := []byte(`{"value":"` + strings.Repeat("a", maxQueuePayloadBytes-len(`{"value":""}`)) + `"}`)
		oversizedJSON := append(append([]byte(nil), atLimitJSON...), ' ')
		if len(atLimitJSON) != maxQueuePayloadBytes || !json.Valid(atLimitJSON) || !json.Valid(oversizedJSON) {
			t.Fatal("invalid queue limit test fixture")
		}

		Enqueue(atLimitJSON)
		requireUsageSubscriberPayload(t, subscriber, string(atLimitJSON))

		Enqueue(oversizedJSON)

		select {
		case got := <-subscriber:
			t.Fatalf("subscriber received oversized payload of %d bytes", len(got))
		default:
		}
	})
}

func TestQueueEvictsOldestItemsAtCountLimit(t *testing.T) {
	withEnabledQueue(t, func() {
		for i := 0; i <= maxQueueItems; i++ {
			Enqueue([]byte(strconv.Itoa(i)))
		}

		items := PopOldest(maxQueueItems + 1)
		if len(items) != maxQueueItems {
			t.Fatalf("PopOldest() returned %d items, want %d", len(items), maxQueueItems)
		}
		if got := string(items[0]); got != "1" {
			t.Fatalf("oldest retained payload = %q, want %q", got, "1")
		}
		if got := string(items[len(items)-1]); got != strconv.Itoa(maxQueueItems) {
			t.Fatalf("newest retained payload = %q, want %q", got, strconv.Itoa(maxQueueItems))
		}
	})
}

func TestQueueEvictsOldestItemsAtTotalByteLimit(t *testing.T) {
	withEnabledQueue(t, func() {
		const payloadBytes = 64 << 10
		payload := bytes.Repeat([]byte{'x'}, payloadBytes)
		itemCount := maxQueueBytes/payloadBytes + 1
		for i := 0; i < itemCount; i++ {
			payload[0] = byte(i)
			Enqueue(payload)
		}

		items := PopOldest(itemCount)
		wantItems := maxQueueBytes / payloadBytes
		if len(items) != wantItems {
			t.Fatalf("PopOldest() returned %d items, want %d", len(items), wantItems)
		}
		if got := items[0][0]; got != 1 {
			t.Fatalf("oldest retained payload marker = %d, want 1", got)
		}
	})
}

func TestSetEnabledFalseClosesUsageSubscribers(t *testing.T) {
	withEnabledQueue(t, func() {
		subscriber, unsubscribe := SubscribeUsage()
		defer unsubscribe()
		errorSubscriber, unsubscribeErrors := SubscribeErrors()
		defer unsubscribeErrors()

		requireUsageSubscriberPayload(t, subscriber, usageSupportRefreshPayload)

		SetEnabled(false)

		select {
		case _, ok := <-subscriber:
			if ok {
				t.Fatalf("subscriber channel remained open after SetEnabled(false)")
			}
		case <-time.After(time.Second):
			t.Fatalf("timeout waiting for subscriber close")
		}

		select {
		case _, ok := <-errorSubscriber:
			if ok {
				t.Fatalf("error subscriber channel remained open after SetEnabled(false)")
			}
		case <-time.After(time.Second):
			t.Fatalf("timeout waiting for error subscriber close")
		}
	})
}

func TestEnqueueErrorBroadcastsToErrorSubscribersAndDiscardsWithoutSubscribers(t *testing.T) {
	withEnabledQueue(t, func() {
		subscriber, unsubscribe := SubscribeErrors()
		defer unsubscribe()

		EnqueueError([]byte("error-record"))
		requireUsageSubscriberPayload(t, subscriber, "error-record")

		unsubscribe()

		EnqueueError([]byte("discarded-error"))
		requireErrorQueueEmpty(t)
	})
}

func TestEnqueueErrorRejectsOversizedPayloadBeforeSubscriberBroadcast(t *testing.T) {
	withEnabledQueue(t, func() {
		subscriber, unsubscribe := SubscribeErrors()
		defer unsubscribe()

		oversizedPayload := bytes.Repeat([]byte{'x'}, maxQueuePayloadBytes+1)
		EnqueueError(oversizedPayload)

		select {
		case got := <-subscriber:
			t.Fatalf("error subscriber received oversized payload of %d bytes", len(got))
		default:
		}

		EnqueueError([]byte(`{"accepted":true}`))
		requireUsageSubscriberPayload(t, subscriber, `{"accepted":true}`)
	})
}

func TestQueueLimitsPreserveRetentionPruning(t *testing.T) {
	previousRetention := retentionSeconds.Load()
	defer retentionSeconds.Store(previousRetention)
	SetRetentionSeconds(int(defaultRetentionSeconds))

	withEnabledQueue(t, func() {
		Enqueue([]byte("expired"))
		global.mu.Lock()
		global.items[global.head].enqueuedAt = time.Now().Add(-time.Duration(defaultRetentionSeconds+1) * time.Second)
		global.mu.Unlock()

		Enqueue([]byte("retained"))

		items := PopOldest(2)
		if len(items) != 1 || string(items[0]) != "retained" {
			t.Fatalf("PopOldest() items = %q, want only retained payload", items)
		}
	})
}

func TestNotifyUsageRefreshBroadcastsOnlyToUsageSubscribers(t *testing.T) {
	withEnabledQueue(t, func() {
		subscriber, unsubscribe := SubscribeUsage()
		defer unsubscribe()
		errorSubscriber, unsubscribeErrors := SubscribeErrors()
		defer unsubscribeErrors()

		requireUsageSubscriberPayload(t, subscriber, usageSupportRefreshPayload)

		NotifyUsageRefresh()
		requireUsageSubscriberPayload(t, subscriber, usageRefreshPayload)

		select {
		case got := <-errorSubscriber:
			t.Fatalf("error subscriber received usage refresh payload %q", string(got))
		default:
		}

		unsubscribe()
		NotifyUsageRefresh()
		if items := PopOldest(1); len(items) != 0 {
			t.Fatalf("PopOldest() items = %q, want empty after refresh notification without subscribers", items)
		}
	})
}

func requireUsageSubscriberPayload(t *testing.T, subscriber <-chan []byte, want string) {
	t.Helper()

	select {
	case got, ok := <-subscriber:
		if !ok {
			t.Fatalf("subscriber closed before receiving %q", want)
		}
		if string(got) != want {
			t.Fatalf("subscriber payload = %q, want %q", string(got), want)
		}
	case <-time.After(time.Second):
		t.Fatalf("timeout waiting for subscriber payload %q", want)
	}
}

func requireErrorQueueEmpty(t *testing.T) {
	t.Helper()

	errorGlobal.mu.Lock()
	defer errorGlobal.mu.Unlock()

	if len(errorGlobal.items)-errorGlobal.head != 0 {
		t.Fatalf("error queue retained %d item(s), want none", len(errorGlobal.items)-errorGlobal.head)
	}
}
