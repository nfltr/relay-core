// Copyright 2026 nfltr
// SPDX-License-Identifier: Apache-2.0

package broker

import (
	"fmt"

	"google.golang.org/protobuf/proto"
)

// DecodeBrokerMessage decodes a broker Message into a typed proto message:
// the payload bytes of one sent with SendBytes, else the proto object sent
// with SendProto (RawObject).
func DecodeBrokerMessage[T proto.Message](msg Message, zero T) (T, error) {
	if payload := msg.Payload(); len(payload) > 0 {
		if err := proto.Unmarshal(payload, zero); err != nil {
			return zero, fmt.Errorf("unmarshalling broker message: %w", err)
		}
		return zero, nil
	}
	if raw := msg.RawObject(); raw != nil {
		if typed, ok := raw.(T); ok {
			return typed, nil
		}
		return zero, fmt.Errorf("unexpected type in broker message: %T", raw)
	}
	return zero, fmt.Errorf("broker message has neither payload nor raw object")
}
