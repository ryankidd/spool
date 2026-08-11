package spool

import (
	"reflect"
	"testing"
)

func TestRecordRoundTrip(t *testing.T) {
	records := []record{
		{kind: kindEnqueue, job: 0, payload: []byte("send welcome email")},
		{kind: kindEnqueue, job: 1 << 40, payload: nil},
		{kind: kindLease, job: 7, lease: 12},
		{kind: kindAck, job: 7, lease: 12},
	}

	for _, want := range records {
		got, err := decodeRecord(want.encode())
		if err != nil {
			t.Fatalf("decodeRecord(%+v): %v", want, err)
		}
		if len(want.payload) == 0 && len(got.payload) == 0 {
			got.payload, want.payload = nil, nil
		}
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("round trip gave %+v, want %+v", got, want)
		}
	}
}

func TestDecodeRecordRejectsMalformed(t *testing.T) {
	tests := map[string][]byte{
		"empty":         {},
		"short header":  {byte(kindAck), 0, 0, 0},
		"unknown kind":  {99, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
		"short lease":   {byte(kindLease), 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0},
		"trailing byte": {byte(kindAck), 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 1, 0},
	}

	for name, b := range tests {
		if _, err := decodeRecord(b); err == nil {
			t.Errorf("decodeRecord(%s) returned nil error, want an error", name)
		}
	}
}
