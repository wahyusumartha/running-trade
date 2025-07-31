package streaming

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kgo"
	"go.uber.org/mock/gomock"
)

func newTestClient(ctrl *gomock.Controller) (*Client, *MockkafkaClient, *Message, *kgo.Record) {
	mockKafka := NewMockkafkaClient(ctrl)
	client := &Client{client: mockKafka}
	msg := &Message{Topic: "test", Key: []byte("k"), Value: []byte("v")}
	record := client.messageToRecord(msg)
	return client, mockKafka, msg, record
}

func TestClient_ProduceSync(t *testing.T) {
	tests := []struct {
		name      string
		setupMock func(mock *MockkafkaClient, ctx context.Context, record *kgo.Record)
		wantErr   error
	}{
		{
			name: "success",
			setupMock: func(mock *MockkafkaClient, ctx context.Context, record *kgo.Record) {
				mock.EXPECT().
					ProduceSync(ctx, record).
					Return(kgo.ProduceResults{{}})
			},
			wantErr: nil,
		},
		{
			name: "failure",
			setupMock: func(mock *MockkafkaClient, ctx context.Context, record *kgo.Record) {
				mock.EXPECT().
					ProduceSync(ctx, record).
					Return(kgo.ProduceResults{{Err: errors.New("produce failed")}})
			},
			wantErr: errors.New("produce failed"),
		},
	}

	for _, tc := range tests {
		tc := tc // capture range variable
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()

			client, mockKafka, msg, record := newTestClient(ctrl)
			ctx := context.Background()
			tc.setupMock(mockKafka, ctx, record)

			err := client.ProduceSync(ctx, msg)
			if tc.wantErr == nil {
				require.NoError(t, err)
			} else {
				require.EqualError(t, err, tc.wantErr.Error())
			}
		})
	}
}
