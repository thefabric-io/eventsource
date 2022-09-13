package eventsource

import (
	"reflect"
	"testing"
)

func TestWithSnapshot(t *testing.T) {
	type args struct {
		frequency int
	}
	tests := []struct {
		name string
		args args
		want *SaveOptions
	}{
		{
			name: "snapshot frequency to 0",
			args: args{frequency: 0},
			want: &SaveOptions{
				WithSnapshot:          false,
				WithSnapshotFrequency: 0,
				MustSendToOutbox:      true,
			},
		},
		{
			name: "snapshot frequency to 10",
			args: args{frequency: 10},
			want: &SaveOptions{
				WithSnapshot:          true,
				WithSnapshotFrequency: 10,
				MustSendToOutbox:      true,
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			opts := NewSaveOptions(WithSnapshot(tt.args.frequency))
			if got := opts; !reflect.DeepEqual(got, tt.want) {
				t.Errorf("WithSnapshot() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestSendToOutbox(t *testing.T) {
	type args struct {
		b bool
	}
	tests := []struct {
		name string
		args args
		want *SaveOptions
	}{
		{
			name: "must send to outbox",
			args: args{b: true},
			want: &SaveOptions{
				WithSnapshot:          true,
				WithSnapshotFrequency: 10,
				MustSendToOutbox:      true,
			},
		},
		{
			name: "must not send to outbox",
			args: args{b: false},
			want: &SaveOptions{
				WithSnapshot:          true,
				WithSnapshotFrequency: 10,
				MustSendToOutbox:      false,
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			opts := NewSaveOptions(SendToOutbox(tt.args.b))
			if got := opts; !reflect.DeepEqual(got, tt.want) {
				t.Errorf("SendToOutbox() = %v, want %v", got, tt.want)
			}
		})
	}
}
