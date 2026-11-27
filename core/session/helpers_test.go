// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package session

import (
	"github.com/nfltr/relay-core/core/session/sessiontest/fake"

	"github.com/nfltr/relay-core/broker"
	"google.golang.org/protobuf/proto"
)

// Compile-time interface checks.
var _ broker.Message = (*testBrokerMessage)(nil)

var _ broker.Producer = (*fake.TestProducer)(nil)

// testBrokerMessage implements broker.Message for testing.
type testBrokerMessage struct {
	payload []byte
	raw     proto.Message
}

func (m *testBrokerMessage) Payload() []byte { return m.payload }

func (m *testBrokerMessage) RawObject() proto.Message { return m.raw }

func (m *testBrokerMessage) Ack() error { return nil }
