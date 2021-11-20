package kinesisdatacounter

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"hash"
	"io"
	"log"
	"strings"
	"time"

	"github.com/aead/siphash"
	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/firehose"
	firehosetypes "github.com/aws/aws-sdk-go-v2/service/firehose/types"
	"github.com/aws/aws-sdk-go-v2/service/kinesis"
	"github.com/clarkduvall/hyperloglog"
)

type KinesisClient interface {
	PutRecord(ctx context.Context, params *kinesis.PutRecordInput, optFns ...func(*kinesis.Options)) (*kinesis.PutRecordOutput, error)
}

type FirehoseClient interface {
	PutRecord(ctx context.Context, params *firehose.PutRecordInput, optFns ...func(*firehose.Options)) (*firehose.PutRecordOutput, error)
}

type App struct {
	cfg            *Config
	kinesisClient  KinesisClient
	firehoseClient FirehoseClient
}

func New(cfg *Config) (*App, error) {
	awsCfg, err := config.LoadDefaultConfig(context.Background())
	if err != nil {
		return nil, err
	}
	return NewWithClient(cfg, kinesis.NewFromConfig(awsCfg), firehose.NewFromConfig(awsCfg)), nil
}

func NewWithClient(cfg *Config, kinesisClient KinesisClient, firehoseClient FirehoseClient) *App {
	return &App{
		cfg:            cfg,
		kinesisClient:  kinesisClient,
		firehoseClient: firehoseClient,
	}
}

type CounterState struct {
	CounterType CounterType `json:"counter_type"`
	RowCount    int64       `json:"row_count,omitempty"`
	Base64HLLPP string      `json:"base64_hllpp,omitempty"`
}

type KinesisTimeWindow struct {
	Start time.Time `json:"start"`
	End   time.Time `json:"end"`
}

type KinesisTimeWindowEvent struct {
	Records                 []events.KinesisEventRecord         `json:"Records"`
	Window                  *KinesisTimeWindow                  `json:"window"`
	State                   map[string]map[string]*CounterState `json:"state"`
	ShardID                 string                              `json:"shardId"`
	EventSourceArn          string                              `json:"eventSourceARN"`
	IsFinalInvokeForWindow  bool                                `json:"isFinalInvokeForWindow"`
	IsWindowTerminatedEarly bool                                `json:"isWindowTerminatedEarly"`
}

type BatchItemFailure struct {
	ItemIdentifier string `json:"itemIdentifier"`
}

type TimeWindowEventResponse struct {
	State             map[string]map[string]*CounterState `json:"state"`
	BatchItemFailures []BatchItemFailure                  `json:"batchItemFailures"`
}

func newTimeWindowEventResponse() *TimeWindowEventResponse {
	return &TimeWindowEventResponse{
		State:             make(map[string]map[string]*CounterState),
		BatchItemFailures: nil,
	}
}

func (resp *TimeWindowEventResponse) MergeInto(other *TimeWindowEventResponse) {
	for id, state := range other.State {
		resp.State[id] = state
	}
	if other.BatchItemFailures != nil {
		resp.AddBatchItemFailures(other.BatchItemFailures...)
	}
}
func (resp *TimeWindowEventResponse) AddBatchItemFailures(items ...BatchItemFailure) {
	if len(items) > 0 {
		if len(resp.BatchItemFailures) == 0 {
			resp.BatchItemFailures = append([]BatchItemFailure{}, items...)
		} else {
			resp.BatchItemFailures = append(resp.BatchItemFailures, items...)
		}
	}
}

func (app *App) Handler(ctx context.Context, event *KinesisTimeWindowEvent) (*TimeWindowEventResponse, error) {
	resp := newTimeWindowEventResponse()
	for _, counter := range app.cfg.Counters {
		if counter.InputStreamARN.Match(event.EventSourceArn) {
			counterResp, err := app.process(ctx, counter, event)
			if err != nil {
				return nil, err
			}
			resp.MergeInto(counterResp)
		}
	}
	return resp, nil
}

const (
	precision = 16
)

func (app *App) process(ctx context.Context, counter *CounterConfig, event *KinesisTimeWindowEvent) (*TimeWindowEventResponse, error) {
	if event.IsWindowTerminatedEarly {
		log.Println("[warn] state over 1MB, isWindowTerminatedEarly == true")
	}
	states, ok := event.State[counter.ID]
	if !ok {
		states = make(map[string]*CounterState, 1)
	}
	state, ok := states[event.ShardID]
	if !ok {
		state = &CounterState{
			CounterType: counter.CounterType,
		}
	}
	resp := newTimeWindowEventResponse()
	records := make([]map[string]interface{}, 0, len(event.Records))
	for _, record := range event.Records {
		var v map[string]interface{}
		if err := json.Unmarshal(record.Kinesis.Data, &v); err != nil {
			log.Printf("[debug] record marshal as json failed: sequence_number=%s err=%s", record.Kinesis.SequenceNumber, err)
			resp.AddBatchItemFailures(BatchItemFailure{
				ItemIdentifier: record.Kinesis.SequenceNumber,
			})
			continue
		}
		records = append(records, v)
	}
	switch counter.CounterType {
	case Count:
		for _, record := range records {
			if record == nil {
				continue
			}
			if counter.TargetColumn != "*" {
				if v, ok := record[counter.TargetColumn]; !ok || v == nil {
					continue
				}
			}
			state.RowCount++
		}
	case ApproxCountDistinct:
		hllpp, err := decodeBase64HLLPP(state.Base64HLLPP)
		if err != nil {
			return nil, err
		}
		h, err := newHasher(counter)
		if err != nil {
			return nil, err
		}
		for _, record := range records {
			if record == nil {
				continue
			}
			v, ok := record[counter.TargetColumn]
			if !ok || v == nil {
				continue
			}
			bs, err := json.Marshal(v)
			if err != nil {
				return nil, fmt.Errorf("can not marshal %#v", v)
			}
			h.Reset()
			h.Write(bs)
			hllpp.Add(h)
		}
		state.Base64HLLPP, err = encodeBase64HLLPP(hllpp)
		if err != nil {
			return nil, err
		}
	default:
		return nil, fmt.Errorf("unknown counter_type=%d", counter.CounterType)
	}
	states[event.ShardID] = state
	resp.State[counter.ID] = states
	if event.IsFinalInvokeForWindow {
		if err := app.putStateRecord(ctx, counter, states, event); err != nil {
			return nil, err
		}
	}
	return resp, nil
}

func (app *App) putStateRecord(ctx context.Context, counter *CounterConfig, states map[string]*CounterState, event *KinesisTimeWindowEvent) error {
	v := map[string]interface{}{
		"window_start": event.Window.Start.UnixMilli(),
		"window_end":   event.Window.End.UnixMilli(),
		"counter_id":   counter.ID,
		"counter_type": counter.CounterType.String(),
	}
	switch counter.CounterType {
	case Count:
		total := int64(0)
		for _, state := range states {
			total += state.RowCount
		}
		v["value"] = total
	case ApproxCountDistinct:
		var hllpp *hyperloglog.HyperLogLogPlus
		for _, state := range states {
			otherHLLPP, err := decodeBase64HLLPP(state.Base64HLLPP)
			if err != nil {
				return err
			}
			if hllpp == nil {
				hllpp = otherHLLPP
			} else {
				if err := hllpp.Merge(otherHLLPP); err != nil {
					return err
				}
			}
		}
		v["value"] = hllpp.Count()
	default:
		return fmt.Errorf("unknown counter_type=%d", counter.CounterType)
	}
	if counter.transformer != nil {
		iter := counter.transformer.RunWithContext(ctx, v)
		for {
			v, ok := iter.Next()
			if !ok {
				break
			}
			if err, ok := v.(error); ok {
				return err
			}
			bs, err := json.Marshal(v)
			if err != nil {
				return err
			}
			if err := app.putRecord(ctx, counter.OutputStreamARN, counter.ID, bs); err != nil {
				return err
			}
		}
		return nil
	}
	bs, err := json.Marshal(v)
	if err != nil {
		return err
	}
	return app.putRecord(ctx, counter.OutputStreamARN, counter.ID, bs)
}

func (app *App) putRecord(ctx context.Context, destinationARN *ARN, partitionKey string, data []byte) error {
	if destinationARN == nil {
		log.Printf("[debug] put record arn is not set partitionKey=%s", partitionKey)
		return nil
	}
	switch destinationARN.Service {
	case "kinesis":
		output, err := app.kinesisClient.PutRecord(ctx, &kinesis.PutRecordInput{
			Data:         data,
			PartitionKey: &partitionKey,
			StreamName:   aws.String(destinationARN.StreamName()),
		})
		if err != nil {
			return err
		}
		log.Printf("[info] put record kinesis data stream=%s sequence_number=%s shard_id=%s", destinationARN.StreamName(), *output.SequenceNumber, *output.ShardId)
	case "firehose":
		output, err := app.firehoseClient.PutRecord(ctx, &firehose.PutRecordInput{
			DeliveryStreamName: aws.String(destinationARN.StreamName()),
			Record: &firehosetypes.Record{
				Data: data,
			},
		})
		if err != nil {
			return err
		}
		log.Printf("[info] put record firehose delivery stream=%s record_id=%s", destinationARN.StreamName(), *output.RecordId)
	}
	return nil
}

func decodeBase64HLLPP(str string) (*hyperloglog.HyperLogLogPlus, error) {
	if str == "" {
		hllpp, err := hyperloglog.NewPlus(precision)
		if err != nil {
			return nil, fmt.Errorf("init HyperLogLog++: %w", err)
		}
		return hllpp, nil
	} else {
		decoder := base64.NewDecoder(base64.StdEncoding, strings.NewReader(str))
		bs, err := io.ReadAll(decoder)
		if err != nil {
			return nil, fmt.Errorf("read state Base64HLLPP: %w", err)
		}
		hllpp := &hyperloglog.HyperLogLogPlus{}
		if err := hllpp.GobDecode(bs); err != nil {
			return nil, fmt.Errorf("restore state Base64HLLPP: %w", err)
		}
		return hllpp, nil
	}
}

func encodeBase64HLLPP(hllpp *hyperloglog.HyperLogLogPlus) (string, error) {
	if hllpp == nil {
		return "", nil
	}
	bs, err := hllpp.GobEncode()
	if err != nil {
		return "", fmt.Errorf("gob encode HyperLogLog++: %w", err)
	}
	var buf bytes.Buffer
	encoder := base64.NewEncoder(base64.StdEncoding, &buf)
	encoder.Write(bs)
	encoder.Close()
	return buf.String(), nil
}

func newHasher(cfg *CounterConfig) (hash.Hash64, error) {
	var key [16]byte
	b, err := hex.DecodeString(cfg.SipHashKeyHex)
	if err != nil {
		return nil, err
	}
	copy(key[:], b)
	return siphash.New64(key[:])
}
