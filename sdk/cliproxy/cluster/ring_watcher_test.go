package cluster

import (
	"context"
	"testing"
)

func TestMembersChanged_SameSetSameWeight(t *testing.T) {
	a := []RingMember{{"sj", 100}, {"la", 100}}
	b := []RingMember{{"la", 100}, {"sj", 100}} // order-insensitive
	if membersChanged(a, b) {
		t.Error("same set same weight should not count as changed")
	}
}

func TestMembersChanged_DifferentCount(t *testing.T) {
	a := []RingMember{{"sj", 100}, {"la", 100}}
	b := []RingMember{{"sj", 100}, {"la", 100}, {"fra", 100}}
	if !membersChanged(a, b) {
		t.Error("different count should count as changed")
	}
}

func TestMembersChanged_WeightOnly(t *testing.T) {
	a := []RingMember{{"sj", 100}}
	b := []RingMember{{"sj", 50}}
	// Weight delta MUST trigger a rebuild — operator bumping weight
	// should take effect within one poll cycle, not wait for membership
	// change.
	if !membersChanged(a, b) {
		t.Error("weight-only change must count as changed")
	}
}

func TestMembersChanged_NodeReplaced(t *testing.T) {
	a := []RingMember{{"sj", 100}, {"la", 100}}
	b := []RingMember{{"sj", 100}, {"fra", 100}}
	if !membersChanged(a, b) {
		t.Error("one node swapped should count as changed")
	}
}

func TestMembersChanged_BothEmpty(t *testing.T) {
	if membersChanged(nil, nil) {
		t.Error("nil/nil should not count as changed")
	}
}

// Run with nil DB must return cleanly (no panic, no hang). Regression
// guard against misconfig.
func TestRingWatcher_NilDB(t *testing.T) {
	w := &RingWatcher{}
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // pre-cancel
	w.Run(ctx)
}

func TestRingWatcher_NilReceiverSafe(t *testing.T) {
	var w *RingWatcher
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	w.Run(ctx) // must not panic
}

func TestDrain_EmptyChannel(t *testing.T) {
	ch := make(chan struct{}, 2)
	drain(ch) // must not block
	ch <- struct{}{}
	ch <- struct{}{}
	drain(ch)
	select {
	case <-ch:
		t.Error("drain should have emptied channel")
	default:
	}
}

func TestFormatMembers(t *testing.T) {
	got := formatMembers([]RingMember{{"sj", 100}, {"la", 50}})
	if got != "sj,la" {
		t.Errorf("unexpected format: %q", got)
	}
	if formatMembers(nil) != "" {
		t.Errorf("empty should produce empty string")
	}
}
