package streaming

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kgo"
	"go.uber.org/mock/gomock"
)

func newTestProducer(ctrl *gomock.Controller) (*Producer, *MockkafkaClient, *Message, *kgo.Record) {
	mockKafka := NewMockkafkaClient(ctrl)
	producer := &Producer{client: mockKafka}
	msg := &Message{Topic: "test", Key: []byte("k"), Value: []byte("v")}
	record := producer.messageToRecord(msg)
	return producer, mockKafka, msg, record
}

func TestProducer_ProduceSync(t *testing.T) {
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

			producer, mockKafka, msg, record := newTestProducer(ctrl)
			ctx := context.Background()
			tc.setupMock(mockKafka, ctx, record)

			err := producer.ProduceSync(ctx, msg)
			if tc.wantErr == nil {
				require.NoError(t, err)
			} else {
				require.EqualError(t, err, tc.wantErr.Error())
			}
		})
	}
}
