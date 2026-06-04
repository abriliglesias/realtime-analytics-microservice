package main

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/segmentio/kafka-go"
)

// -------------------------------------------------------------------
// Mock Kafka writer
// -------------------------------------------------------------------

// mockKafkaWriter satisfies the KafkaWriter interface and records the last
// messages written. Set writeErr to simulate a broker-side failure.
type mockKafkaWriter struct {
	writeErr    error
	writtenMsgs []kafka.Message
}

func (m *mockKafkaWriter) WriteMessages(ctx context.Context, msgs ...kafka.Message) error {
	if m.writeErr != nil {
		return m.writeErr
	}
	m.writtenMsgs = append(m.writtenMsgs, msgs...)
	return nil
}

// -------------------------------------------------------------------
// Helper: build a request that has passed through TraceMiddleware
// -------------------------------------------------------------------

func newRequestWithTrace(method, target, body string) *http.Request {
	req := httptest.NewRequest(method, target, strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")

	// Simulate what TraceMiddleware does: inject a trace_id into the context.
	ctx := context.WithValue(req.Context(), traceIDKey, "test-trace-id-001")
	return req.WithContext(ctx)
}

// -------------------------------------------------------------------
// POST /event — table-driven tests
// -------------------------------------------------------------------

func TestHandleEvent(t *testing.T) {
	tests := []struct {
		name           string
		body           string
		kafkaErr       error
		wantStatusCode int
		wantMsgCount   int
	}{
		{
			name:           "valid payload is accepted and enqueued",
			body:           `{"user_id":"user-abc","event_type":"page_view"}`,
			wantStatusCode: http.StatusAccepted,
			wantMsgCount:   1,
		},
		{
			name:           "missing user_id returns 400",
			body:           `{"event_type":"page_view"}`,
			wantStatusCode: http.StatusBadRequest,
			wantMsgCount:   0,
		},
		{
			name:           "empty user_id returns 400",
			body:           `{"user_id":"","event_type":"page_view"}`,
			wantStatusCode: http.StatusBadRequest,
			wantMsgCount:   0,
		},
		{
			name:           "empty body returns 400",
			body:           ``,
			wantStatusCode: http.StatusBadRequest,
			wantMsgCount:   0,
		},
		{
			name:           "malformed JSON returns 400",
			body:           `{"user_id":}`,
			wantStatusCode: http.StatusBadRequest,
			wantMsgCount:   0,
		},
		{
			name:           "kafka write failure returns 500",
			body:           `{"user_id":"user-abc","event_type":"page_view"}`,
			kafkaErr:       errors.New("broker unavailable"),
			wantStatusCode: http.StatusInternalServerError,
			wantMsgCount:   0,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			mock := &mockKafkaWriter{writeErr: tc.kafkaErr}
			handler := NewEventHandler(mock, nil) // db=nil; not used by HandleEvent

			req := newRequestWithTrace(http.MethodPost, "/event", tc.body)
			rec := httptest.NewRecorder()

			handler.HandleEvent(rec, req)

			if rec.Code != tc.wantStatusCode {
				t.Errorf("status: got %d, want %d (body: %q)", rec.Code, tc.wantStatusCode, rec.Body.String())
			}
			if len(mock.writtenMsgs) != tc.wantMsgCount {
				t.Errorf("kafka messages written: got %d, want %d", len(mock.writtenMsgs), tc.wantMsgCount)
			}
		})
	}
}

// TestHandleEvent_TraceIDPropagatedToKafkaHeader verifies that a valid request
// embeds the trace_id from context into the outgoing Kafka message header.
func TestHandleEvent_TraceIDPropagatedToKafkaHeader(t *testing.T) {
	mock := &mockKafkaWriter{}
	handler := NewEventHandler(mock, nil)

	req := newRequestWithTrace(http.MethodPost, "/event", `{"user_id":"u1","event_type":"page_view"}`)
	rec := httptest.NewRecorder()
	handler.HandleEvent(rec, req)

	if rec.Code != http.StatusAccepted {
		t.Fatalf("expected 202, got %d", rec.Code)
	}

	if len(mock.writtenMsgs) != 1 {
		t.Fatalf("expected 1 kafka message, got %d", len(mock.writtenMsgs))
	}

	var traceHeader string
	for _, h := range mock.writtenMsgs[0].Headers {
		if h.Key == "trace_id" {
			traceHeader = string(h.Value)
		}
	}

	if traceHeader != "test-trace-id-001" {
		t.Errorf("trace_id header: got %q, want %q", traceHeader, "test-trace-id-001")
	}
}

// TestHandleEvent_PayloadTooLarge verifies that bodies exceeding 1MB are
// rejected before they can exhaust server memory.
func TestHandleEvent_PayloadTooLarge(t *testing.T) {
	mock := &mockKafkaWriter{}
	handler := NewEventHandler(mock, nil)

	// Build a body that is 1MB + 1 byte — just over the limit.
	oversized := strings.Repeat("x", maxBodyBytes+1)
	req := newRequestWithTrace(http.MethodPost, "/event", oversized)
	rec := httptest.NewRecorder()

	handler.HandleEvent(rec, req)

	if rec.Code != http.StatusRequestEntityTooLarge {
		t.Errorf("expected 413, got %d", rec.Code)
	}
}

// -------------------------------------------------------------------
// validateEvent — unit tests (pure function, no HTTP machinery)
// -------------------------------------------------------------------

func TestValidateEvent(t *testing.T) {
	tests := []struct {
		name    string
		input   []byte
		wantErr bool
	}{
		{"valid", []byte(`{"user_id":"u1","event_type":"page_view"}`), false},
		{"empty body", []byte(``), true},
		{"nil body", nil, true},
		{"missing user_id field", []byte(`{"event_type":"page_view"}`), true},
		{"empty string user_id", []byte(`{"user_id":""}`), true},
		{"malformed json", []byte(`{bad}`), true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := validateEvent(tc.input)
			if tc.wantErr && err == nil {
				t.Error("expected error, got nil")
			}
			if !tc.wantErr && err != nil {
				t.Errorf("expected no error, got: %v", err)
			}
		})
	}
}
