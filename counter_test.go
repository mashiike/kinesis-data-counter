package kinesisdatacounter_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"math/rand"
	"testing"
	"time"

	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/firehose"
	"github.com/aws/aws-sdk-go-v2/service/kinesis"
	kinesisdatacounter "github.com/mashiike/kinesis-data-counter"
	"github.com/stretchr/testify/require"
)

type counterTestCase struct {
	casename       string
	config         string
	expectedFormat string
	expectedValue  int64
}

func (c counterTestCase) doTest(t *testing.T, n int, events []*kinesisdatacounter.KinesisTimeWindowEvent) {
	t.Run(fmt.Sprintf("%s-%d", c.casename, n), func(t *testing.T) {
		var buf bytes.Buffer
		app := buildApp(t, c.config, &buf)
		ctx := context.Background()
		var state map[string]map[string]*kinesisdatacounter.CounterState
		for _, event := range events {
			event.State = state
			resp, err := app.Handler(ctx, event)
			require.NoError(t, err)
			state = resp.State
		}
		var actual map[string]interface{}
		err := json.Unmarshal(buf.Bytes(), &actual)
		require.NoError(t, err, "result unmarshal")
		interfaceValue, ok := actual["value"]
		require.True(t, ok, "must set `value` key")
		value, ok := interfaceValue.(float64)
		require.True(t, ok, "must convert value float64")
		actualValue := int64(value)
		expected := fmt.Sprintf(c.expectedFormat, actualValue)
		t.Logf("expected: %s", expected)
		t.Logf("actual  : %s", buf.String())
		require.JSONEq(t, expected, buf.String())
		require.InEpsilon(t, c.expectedValue, actualValue, 0.05, "must in epsilon 0.05")
	})
}

var inputStream = "arn:aws:kinesis:ap-northeast-1:111122223333:stream/input-stream"

func TestCounter(t *testing.T) {
	cases := []counterTestCase{
		{
			casename:       "count-request_id",
			config:         "testdata/config.yaml",
			expectedFormat: `{"counter_id":"request_count","counter_type":"count","value":%d,"window_end":1638357600000,"window_start":1638357540000}`,
			expectedValue:  -1,
		},
		{
			casename:       "approx_count_distinct-user_id",
			config:         "testdata/approx_count_distinct.yaml",
			expectedFormat: `{"counter_id":"unique_user_count","counter_type":"approx_count_distinct","value":%d,"window_end":1638357600000,"window_start":1638357540000}`,
			expectedValue:  -2,
		},
	}
	for _, m := range []int{10, 100, 200, 500} {
		for _, n := range []int{1000, 2000, 4000, 8000} {
			events := createEvents(t, inputStream, n, m)
			for _, c := range cases {
				if c.expectedValue == -1 {
					c.expectedValue = int64(n)
				}
				if c.expectedValue == -2 {
					c.expectedValue = int64(m)
				}
				c.doTest(t, n, events)
			}
		}
	}
}

var r *rand.Rand = rand.New(rand.NewSource(time.Now().UnixNano()))

func createEvents(t *testing.T, arn string, n int, m int) []*kinesisdatacounter.KinesisTimeWindowEvent {
	t.Helper()
	userIDs := make([]int64, 0, m)
	current := int64(1000)
	for i := 0; i < m; i++ {
		current += int64(r.Intn(3) + 1)
		userIDs = append(userIDs, current)
	}
	rand.Shuffle(m, func(i, j int) { userIDs[i], userIDs[j] = userIDs[j], userIDs[i] })
	j := 0
	records := make([]events.KinesisEventRecord, 0, n)
	baseTime := time.Date(2021, 12, 1, 11, 19, 00, 00, time.UTC)
	windowStart := baseTime
	windowEnd := baseTime.Add(time.Minute)
	tick := time.Minute / time.Duration(n)
	for i := 0; i < n; i++ {
		if j >= m {
			j = 0
			rand.Shuffle(m, func(k, l int) { userIDs[k], userIDs[l] = userIDs[l], userIDs[k] })
		}
		data, err := json.Marshal(map[string]interface{}{
			"time":       baseTime.Add(time.Duration(i) * tick),
			"request_id": i + 1000,
			"user_id":    userIDs[j],
		})
		if err != nil {
			t.Fatal(err)
		}
		records = append(records, events.KinesisEventRecord{
			EventSourceArn: arn,
			Kinesis: events.KinesisRecord{
				Data: data,
			},
		})
		j++
	}
	shardCount := r.Intn(2) + 1
	var currentEvents []*kinesisdatacounter.KinesisTimeWindowEvent
	oneEventSize := 1000
	events := make([]*kinesisdatacounter.KinesisTimeWindowEvent, 0, shardCount*n/oneEventSize)

	for i := 0; i < n; i++ {
		if i/oneEventSize == 0 {
			if currentEvents != nil {
				events = append(events, currentEvents...)
			}
			currentEvents = make([]*kinesisdatacounter.KinesisTimeWindowEvent, shardCount)
			for j := 0; j < shardCount; j++ {
				currentEvents[j] = &kinesisdatacounter.KinesisTimeWindowEvent{
					Records: nil,
					Window: &kinesisdatacounter.KinesisTimeWindow{
						Start: windowStart,
						End:   windowEnd,
					},
					EventSourceArn: arn,
					ShardID:        fmt.Sprintf("Shard-0000%02d", j),
				}
			}
		}
		shard := r.Intn(shardCount)
		currentEvents[shard].Records = append(currentEvents[shard].Records, records[i])
	}
	events = append(events, currentEvents...)
	events[len(events)-1].IsFinalInvokeForWindow = true
	return events
}

func buildApp(t *testing.T, path string, buf *bytes.Buffer) *kinesisdatacounter.App {
	t.Helper()
	cfg := kinesisdatacounter.NewDefaultConfig()
	if err := cfg.Load(path); err != nil {
		t.Fatal(err)
	}
	return kinesisdatacounter.NewWithClient(
		cfg,
		&mockKinesisClient{
			buf: buf,
		},
		&mockFirestoreClient{
			buf: buf,
		},
	)
}

type mockKinesisClient struct {
	buf *bytes.Buffer
}

func (m *mockKinesisClient) PutRecord(ctx context.Context, params *kinesis.PutRecordInput, optFns ...func(*kinesis.Options)) (*kinesis.PutRecordOutput, error) {
	m.buf.Write(params.Data)
	return &kinesis.PutRecordOutput{
		SequenceNumber: aws.String("dummy"),
		ShardId:        aws.String("dummy"),
	}, nil
}

type mockFirestoreClient struct {
	buf *bytes.Buffer
}

func (m *mockFirestoreClient) PutRecord(ctx context.Context, params *firehose.PutRecordInput, optFns ...func(*firehose.Options)) (*firehose.PutRecordOutput, error) {
	m.buf.Write(params.Record.Data)
	return &firehose.PutRecordOutput{
		RecordId: aws.String("dummy"),
	}, nil
}
