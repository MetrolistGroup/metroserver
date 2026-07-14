package server

import (
	"testing"
	"time"

	"go.uber.org/zap"
)

func TestClientAllowMessageRateLimit(t *testing.T) {
	client := newClient("client", nil)
	start := time.Unix(100, 0)

	for i := 0; i < MaxMessagesPerWindow; i++ {
		if !client.allowMessage(start) {
			t.Fatalf("message %d was rejected before the limit", i+1)
		}
	}
	if client.allowMessage(start.Add(MessageRateWindow - time.Nanosecond)) {
		t.Fatal("message above the limit was accepted in the same window")
	}
	if !client.allowMessage(start.Add(MessageRateWindow)) {
		t.Fatal("first message in a new window was rejected")
	}
}

func TestClientCloseSendIsIdempotent(t *testing.T) {
	client := newClient("client", nil)
	client.closeSend()
	client.closeSend()

	if !client.isClosed() {
		t.Fatal("client was not marked closed")
	}
	if _, open := <-client.Send; open {
		t.Fatal("send channel was not closed")
	}
}

func TestClientClosesWhenSendBufferIsFull(t *testing.T) {
	client := newClient("client", nil)
	for i := 0; i < cap(client.Send); i++ {
		client.Send <- nil
	}

	client.sendMessage(zap.NewNop(), MsgTypePong, nil)
	if !client.isClosed() {
		t.Fatal("client with a full send buffer was not closed")
	}
}
