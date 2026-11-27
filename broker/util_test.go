// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package broker

import (
	"context"
	"strings"
	"testing"

	"github.com/nfltr/relay/proto/pb"
	"google.golang.org/protobuf/proto"
)

// testBrokerMessage implements Message for testing
type testBrokerMessage struct {
	payload []byte
	raw     proto.Message
	ackErr  error
}

func (m *testBrokerMessage) Payload() []byte          { return m.payload }
func (m *testBrokerMessage) RawObject() proto.Message { return m.raw }
func (m *testBrokerMessage) Ack() error               { return m.ackErr }

// testProducer implements Producer for testing
type testProducer struct {
	lastData any
	sendErr  error
}

const testMsgID = "msg-id"

func (p *testProducer) SendBytes(_ context.Context, data []byte) (string, error) {
	p.lastData = data
	return testMsgID, p.sendErr
}
func (p *testProducer) SendProto(_ context.Context, msg proto.Message) (string, error) {
	p.lastData = msg
	return testMsgID, p.sendErr
}
func (p *testProducer) Close() {}

// --- DecodeBrokerMessage tests ---

func TestDecodeBrokerMessage_InMemory_Success(t *testing.T) {
	expected := &pb.HTTPProxyRequest{
		Request: &pb.HTTPRequest{TransactionId: "txn-1"},
	}
	msg := &testBrokerMessage{raw: expected}

	result, err := DecodeBrokerMessage(msg, &pb.HTTPProxyRequest{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.Request.TransactionId != "txn-1" {
		t.Errorf("expected txn-1, got %s", result.Request.TransactionId)
	}
}

func TestDecodeBrokerMessage_InMemory_WrongType(t *testing.T) {
	// Send a different proto type than expected
	msg := &testBrokerMessage{raw: &pb.HTTPProxyResponse{}}

	_, err := DecodeBrokerMessage(msg, &pb.HTTPProxyRequest{})
	if err == nil {
		t.Fatal("expected error for wrong type")
	}
	if !strings.Contains(err.Error(), "unexpected type") {
		t.Errorf("expected 'unexpected type' in error, got: %v", err)
	}
}

func TestDecodeBrokerMessage_Bytes_Success(t *testing.T) {
	original := &pb.HTTPProxyResponse{
		Response: &pb.HTTPResponse{TransactionId: "txn-2"},
		Routing:  &pb.RoutingInfo{AgentId: "oph-1"},
	}
	bytes, err := proto.Marshal(original)
	if err != nil {
		t.Fatalf("failed to marshal: %v", err)
	}
	msg := &testBrokerMessage{payload: bytes, raw: nil}

	result, err := DecodeBrokerMessage(msg, &pb.HTTPProxyResponse{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.Response.TransactionId != "txn-2" {
		t.Errorf("expected txn-2, got %s", result.Response.TransactionId)
	}
}

func TestDecodeBrokerMessage_Bytes_InvalidPayload(t *testing.T) {
	msg := &testBrokerMessage{payload: []byte("not-valid-proto"), raw: nil}

	_, err := DecodeBrokerMessage(msg, &pb.HTTPProxyRequest{})
	if err == nil {
		t.Fatal("expected error for invalid payload")
	}
	if !strings.Contains(err.Error(), "unmarshalling broker message") {
		t.Errorf("expected 'unmarshalling broker message' in error, got: %v", err)
	}
}

func TestDecodeBrokerMessage_HTTPProxyResponse_InMemory(t *testing.T) {
	expected := &pb.HTTPProxyResponse{
		Response: &pb.HTTPResponse{TransactionId: "txn-3"},
	}
	msg := &testBrokerMessage{raw: expected}

	result, err := DecodeBrokerMessage(msg, &pb.HTTPProxyResponse{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result != expected {
		t.Error("expected same pointer")
	}
}

// TestDecodeBrokerMessage_NeitherPayloadNorRawObject verifies error when both are empty.
func TestDecodeBrokerMessage_NeitherPayloadNorRawObject(t *testing.T) {
	msg := &testBrokerMessage{payload: nil, raw: nil}

	_, err := DecodeBrokerMessage(msg, &pb.HTTPProxyRequest{})
	if err == nil {
		t.Fatal("expected error for empty message")
	}
	if !strings.Contains(err.Error(), "neither payload nor raw object") {
		t.Errorf("expected 'neither payload nor raw object' in error, got: %v", err)
	}
}

// Verify interface compliance at compile time
var _ Message = (*testBrokerMessage)(nil)
var _ Producer = (*testProducer)(nil)
