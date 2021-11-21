package kinesisdatacounter

import (
	"context"
	"fmt"
	"io"
	"log"
	"time"

	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/kinesis"
	kinesistypes "github.com/aws/aws-sdk-go-v2/service/kinesis/types"
	"golang.org/x/sync/errgroup"
)

func (app *App) Run(ctx context.Context, streamName string, tumblingWindow time.Duration) error {
	describeOutput, err := app.kinesisClient.DescribeStream(ctx, &kinesis.DescribeStreamInput{
		StreamName: aws.String(streamName),
	})
	if err != nil {
		return fmt.Errorf("describe stream: %w", err)
	}
	eg, egctx := errgroup.WithContext(ctx)

	shardIDs := describeOutput.StreamDescription.Shards
	for _, shardID := range shardIDs {
		eg.Go(func() error {
			return app.getRecords(egctx, &getRecordInput{
				streamARN:      *describeOutput.StreamDescription.StreamARN,
				streamName:     streamName,
				shardID:        *shardID.ShardId,
				tumblingWindow: tumblingWindow,
			})
		})
	}
	if err := eg.Wait(); err != nil {
		return err
	}
	return nil
}

type getRecordInput struct {
	streamARN      string
	streamName     string
	shardID        string
	tumblingWindow time.Duration
}

const (
	bufRecordsCap = 1000
)

var (
	getRecordInterval = time.Second
)

func (app *App) getRecords(ctx context.Context, params *getRecordInput) error {
	iterOutput, err := app.kinesisClient.GetShardIterator(ctx, &kinesis.GetShardIteratorInput{
		StreamName:        aws.String(params.streamName),
		ShardId:           aws.String(params.shardID),
		ShardIteratorType: kinesistypes.ShardIteratorTypeLatest,
	})
	if err != nil {
		return err
	}
	iter := iterOutput.ShardIterator
	state := newInvokeState(params, time.Now())
	bufRecords := make([]kinesistypes.Record, 0, bufRecordsCap+1)
	log.Printf("[debug] stream=%s shard=%s start iterate", params.streamName, params.shardID)
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
		recordOutput, err := app.kinesisClient.GetRecords(ctx, &kinesis.GetRecordsInput{
			ShardIterator: iter,
			Limit:         aws.Int32(bufRecordsCap),
		})
		log.Printf("[debug] stream=%s shard=%s get record len=%d", params.streamName, params.shardID, len(recordOutput.Records))
		if err != nil {
			return err
		}
		iter = recordOutput.NextShardIterator
		for _, record := range recordOutput.Records {
			if len(bufRecords) >= bufRecordsCap {
				state.isFinalInvokeForWindow = true
				state, err = app.invoke(ctx, state, bufRecords)
				if err != nil {
					return err
				}
				bufRecords = make([]kinesistypes.Record, 0, bufRecordsCap+1)
			}
			if state.isNeedInvoke(*record.ApproximateArrivalTimestamp) {
				state.isFinalInvokeForWindow = true
				_, err = app.invoke(ctx, state, bufRecords)
				if err != nil {
					return err
				}
				state = newInvokeState(params, state.windowEnd)
				bufRecords = make([]kinesistypes.Record, 0, bufRecordsCap+1)
			}
			bufRecords = append(bufRecords, record)
		}
		if len(recordOutput.Records) == 0 {
			if state.isNeedInvoke(time.Now()) {
				state.isFinalInvokeForWindow = true
				_, err = app.invoke(ctx, state, bufRecords)
				if err != nil {
					return err
				}
				state = newInvokeState(params, state.windowEnd)
				bufRecords = make([]kinesistypes.Record, 0, bufRecordsCap)
			}
			time.Sleep(getRecordInterval)
		}
	}
}

type invokeState struct {
	streamARN              string
	streamName             string
	shardID                string
	windowStart            time.Time
	windowEnd              time.Time
	tumblingWindow         time.Duration
	state                  map[string]map[string]*CounterState
	isFinalInvokeForWindow bool
}

func (s *invokeState) isNeedInvoke(t time.Time) bool {
	return s.windowEnd.Before(t)
}

func newInvokeState(params *getRecordInput, t time.Time) *invokeState {
	windowStart := t.Truncate(params.tumblingWindow)
	windowEnd := windowStart.Add(params.tumblingWindow)
	return &invokeState{
		streamARN:      params.streamARN,
		streamName:     params.streamName,
		shardID:        params.shardID,
		windowStart:    windowStart,
		windowEnd:      windowEnd,
		tumblingWindow: params.tumblingWindow,
	}
}

func (app *App) invoke(ctx context.Context, state *invokeState, records []kinesistypes.Record) (*invokeState, error) {
	log.Printf("[debug] stream=%s shard=%s state is %#v", state.streamName, state.shardID, state.state)
	log.Printf("[debug] stream=%s shard=%s num of records is %d", state.streamName, state.shardID, len(records))
	log.Printf("[debug] stream=%s shard=%s window %s ~ %s", state.streamName, state.shardID, state.windowStart, state.windowEnd)
	eventRecords := make([]events.KinesisEventRecord, 0, len(records))
	for _, record := range records {
		eventRecords = append(eventRecords, events.KinesisEventRecord{
			EventSourceArn: state.streamARN,
			Kinesis: events.KinesisRecord{
				ApproximateArrivalTimestamp: events.SecondsEpochTime{
					Time: *record.ApproximateArrivalTimestamp,
				},
				Data:           record.Data,
				EncryptionType: string(record.EncryptionType),
				PartitionKey:   *record.PartitionKey,
				SequenceNumber: *record.SequenceNumber,
			},
		})
	}
	log.Printf("[debug] stream=%s shard=%s try invoke", state.streamName, state.shardID)
	resp, err := app.Handler(ctx, &KinesisTimeWindowEvent{
		Records: eventRecords,
		Window: &KinesisTimeWindow{
			Start: state.windowStart,
			End:   state.windowEnd,
		},
		State:                  state.state,
		ShardID:                state.shardID,
		EventSourceArn:         state.streamARN,
		IsFinalInvokeForWindow: state.isFinalInvokeForWindow,
	})
	if err != nil {
		log.Printf("[debug] stream=%s shard=%s invoke failed %s", state.streamName, state.shardID, err)
		return nil, err
	}
	state.state = resp.State
	log.Printf("[debug] stream=%s shard=%s invoke success after state=%#v", state.streamName, state.shardID, state.state)
	return state, nil
}

func (app *App) SetOutput(w io.Writer) {
	app.output = w
}
