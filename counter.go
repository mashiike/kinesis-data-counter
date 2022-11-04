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
	"sync"
	"time"

	"github.com/aead/siphash"
	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/firehose"
	firehosetypes "github.com/aws/aws-sdk-go-v2/service/firehose/types"
	"github.com/aws/aws-sdk-go-v2/service/kinesis"
	"github.com/clarkduvall/hyperloglog"
	"github.com/mashiike/evaluator"
	"golang.org/x/sync/errgroup"
)

type KinesisClient interface {
	GetRecords(ctx context.Context, params *kinesis.GetRecordsInput, optFns ...func(*kinesis.Options)) (*kinesis.GetRecordsOutput, error)
	GetShardIterator(ctx context.Context, params *kinesis.GetShardIteratorInput, optFns ...func(*kinesis.Options)) (*kinesis.GetShardIteratorOutput, error)
	DescribeStream(ctx context.Context, params *kinesis.DescribeStreamInput, optFns ...func(*kinesis.Options)) (*kinesis.DescribeStreamOutput, error)
	PutRecord(ctx context.Context, params *kinesis.PutRecordInput, optFns ...func(*kinesis.Options)) (*kinesis.PutRecordOutput, error)
}

type FirehoseClient interface {
	PutRecord(ctx context.Context, params *firehose.PutRecordInput, optFns ...func(*firehose.Options)) (*firehose.PutRecordOutput, error)
}

type App struct {
	cfg              *Config
	kinesisClient    KinesisClient
	firehoseClient   FirehoseClient
	output           io.Writer
	version          string
	aggregateChannel chan IntermediateRecord
	ignorePutRecord  bool
}

func (app *App) SetOutput(w io.Writer) {
	app.output = w
}

func (app *App) SetVersion(version string) {
	app.version = version
}

func (app *App) SetIgnorePutRecord(f bool) {
	app.ignorePutRecord = f
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

type CounterStates map[string]map[string]*CounterState

type KinesisTimeWindow struct {
	Start time.Time `json:"start"`
	End   time.Time `json:"end"`
}

func (w *KinesisTimeWindow) String() string {
	if w == nil {
		return "[now window]"
	}
	return fmt.Sprintf("%s ~ %s", w.Start, w.End)
}

type KinesisTimeWindowEvent struct {
	Records                 []events.KinesisEventRecord `json:"Records"`
	Window                  *KinesisTimeWindow          `json:"window"`
	State                   CounterStates               `json:"state"`
	ShardID                 string                      `json:"shardId"`
	EventSourceArn          string                      `json:"eventSourceARN"`
	IsFinalInvokeForWindow  bool                        `json:"isFinalInvokeForWindow"`
	IsWindowTerminatedEarly bool                        `json:"isWindowTerminatedEarly"`
}

type BatchItemFailure struct {
	ItemIdentifier string `json:"itemIdentifier"`
}

type TimeWindowEventResponse struct {
	mu                sync.Mutex
	State             CounterStates      `json:"state"`
	BatchItemFailures []BatchItemFailure `json:"batchItemFailures"`
}

func newTimeWindowEventResponse() *TimeWindowEventResponse {
	return &TimeWindowEventResponse{
		State:             make(CounterStates),
		BatchItemFailures: nil,
	}
}

func (s CounterStates) MergeInto(other CounterStates) {
	for id, state := range other {
		s[id] = state
	}
}

func (resp *TimeWindowEventResponse) MergeInto(other *TimeWindowEventResponse) {
	resp.mu.Lock()
	defer resp.mu.Unlock()
	resp.State.MergeInto(other.State)
	if other.BatchItemFailures != nil {
		resp.AddBatchItemFailures(other.BatchItemFailures...)
	}
}
func (resp *TimeWindowEventResponse) AddBatchItemFailures(items ...BatchItemFailure) {
	resp.mu.Lock()
	defer resp.mu.Unlock()
	if len(items) > 0 {
		if len(resp.BatchItemFailures) == 0 {
			resp.BatchItemFailures = append([]BatchItemFailure{}, items...)
		} else {
			resp.BatchItemFailures = append(resp.BatchItemFailures, items...)
		}
	}
}
func (app *App) Handler(ctx context.Context, event *KinesisTimeWindowEvent) (*TimeWindowEventResponse, error) {
	log.Printf("[info] start handler event_source_arn=%s shard_id=%s", event.EventSourceArn, event.ShardID)
	resp, err := app.handler(ctx, event)
	if err != nil {
		log.Printf("[error] lambda function return: %s", err)
		return nil, err
	}
	log.Printf("[info] end handler event_source_arn=%s shard_id=%s batch_items_failures=%d", event.EventSourceArn, event.ShardID, len(resp.BatchItemFailures))
	return resp, nil
}

func (app *App) handler(ctx context.Context, event *KinesisTimeWindowEvent) (*TimeWindowEventResponse, error) {
	var err error
	log.Printf("[debug] start deaggregate")
	event.Records, err = app.deaggregate(ctx, event.Records)
	if err != nil {
		log.Printf("[debug] failed deaggregate: %s", err)
		return nil, err
	}
	log.Printf("[debug] end deaggregate")
	resp := newTimeWindowEventResponse()
	eg, egctx := errgroup.WithContext(ctx)
	for _, c := range app.cfg.Counters {
		counter := c
		if counter.InputStreamARN.Match(event.EventSourceArn) {
			log.Printf("[info] match counter=%s source=%s", counter.ID, event.EventSourceArn)
			eg.Go(func() error {
				counterResp, err := app.process(egctx, counter, event)
				if err != nil {
					return err
				}
				resp.MergeInto(counterResp)
				return nil
			})
		}
		if counter.AggregateStreamArn != nil {
			if counter.AggregateStreamArn.Match(event.EventSourceArn) {
				log.Printf("[info] match as aggregate counter=%s source=%s", counter.ID, event.EventSourceArn)
				eg.Go(func() error {
					counterResp, err := app.aggregateProcess(egctx, counter, event)
					if err != nil {
						return err
					}
					resp.MergeInto(counterResp)
					return nil
				})
			}
		}
	}
	if err := eg.Wait(); err != nil {
		log.Printf("[debug] eg.Wait: %s", err)
		return nil, err
	}
	return resp, nil
}

const (
	precision = 16
)

func (app *App) getState(counter *CounterConfig, event *KinesisTimeWindowEvent) *CounterState {
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
	return state
}

func (app *App) setState(counter *CounterConfig, event *KinesisTimeWindowEvent, state *CounterState) map[string]*CounterState {
	states, ok := event.State[counter.ID]
	if !ok {
		states = make(map[string]*CounterState, 1)
	}
	states[event.ShardID] = state
	return states
}

func (app *App) process(ctx context.Context, counter *CounterConfig, event *KinesisTimeWindowEvent) (*TimeWindowEventResponse, error) {
	if event.IsWindowTerminatedEarly {
		log.Println("[warn] state over 1MB, isWindowTerminatedEarly == true")
	}
	state := app.getState(counter, event)
	resp := newTimeWindowEventResponse()
	records := make([]map[string]interface{}, 0, len(event.Records))
	log.Printf("[info] process %d records window %s", len(event.Records), event.Window)
	for _, record := range event.Records {
		var v map[string]interface{}
		if err := json.Unmarshal(record.Kinesis.Data, &v); err != nil {
			log.Printf("[debug] record unmarshal as json failed: sequence_number=%s err=%s", record.Kinesis.SequenceNumber, err)
			resp.AddBatchItemFailures(BatchItemFailure{
				ItemIdentifier: record.Kinesis.SequenceNumber,
			})
			continue
		}
		log.Printf("[debug] record = %#v", v)
		records = append(records, v)
	}
	switch counter.CounterType {
	case Count:
		log.Println("[debug] start count")
		for _, record := range records {
			if record == nil {
				continue
			}
			if counter.evaluator != nil {
				val, err := counter.evaluator.Eval(evaluator.Variables(record))
				if err != nil {
					bs, _ := json.Marshal(record)
					log.Printf("[warn] eval failed %s, record=%s", err, string(bs))
					continue
				}
				if val == nil {
					continue
				}
				if b, ok := val.(bool); ok && !b {
					continue
				}
			} else if counter.TargetColumn != "*" {
				if v, ok := record[counter.TargetColumn]; !ok || v == nil {
					continue
				}
			}
			state.RowCount++
		}
		log.Println("[debug] end count")
	case ApproxCountDistinct:
		log.Println("[debug] start approx count distinct")
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
			var v interface{}
			if counter.evaluator != nil {
				var err error
				v, err = counter.evaluator.Eval(evaluator.Variables(record))
				if err != nil {
					bs, _ := json.Marshal(record)
					log.Printf("[warn] eval failed %s, record=%s", err, string(bs))
					continue
				}
				if v == nil {
					continue
				}
			} else {
				var ok bool
				v, ok = record[counter.TargetColumn]
				if !ok || v == nil {
					continue
				}
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
		log.Println("[debug] end approx count distinct")
	default:
		log.Printf("[debug] unknown counter_type=%s", counter.CounterType)
		return nil, fmt.Errorf("unknown counter_type=%d", counter.CounterType)
	}
	if counter.AggregateStreamArn != nil || app.aggregateChannel != nil {
		log.Printf("[debug] put intermediate record counter=%s", counter.ID)
		if err := app.putIntermediateRecord(ctx, counter, state, event); err != nil {
			return nil, err
		}
		return resp, nil
	}
	resp.State[counter.ID] = app.setState(counter, event, state)
	if event.IsFinalInvokeForWindow {
		log.Printf("[debug] final invoke counter=%s", counter.ID)
		if err := app.putStateRecord(ctx, counter, state, event); err != nil {
			return nil, err
		}
	}
	return resp, nil
}

func (app *App) aggregateProcess(ctx context.Context, counter *CounterConfig, event *KinesisTimeWindowEvent) (*TimeWindowEventResponse, error) {
	if event.IsWindowTerminatedEarly {
		log.Println("[warn] state over 1MB, isWindowTerminatedEarly == true")
	}
	state := app.getState(counter, event)
	resp := newTimeWindowEventResponse()
	records := make([]*IntermediateRecord, 0, len(event.Records))
	log.Printf("[info] aggregate process %d records window %s", len(event.Records), event.Window)
	for _, record := range event.Records {
		var v IntermediateRecord
		if err := json.Unmarshal(record.Kinesis.Data, &v); err != nil {
			log.Printf("[debug] record unmarshal as json failed: sequence_number=%s err=%s", record.Kinesis.SequenceNumber, err)
			resp.AddBatchItemFailures(BatchItemFailure{
				ItemIdentifier: record.Kinesis.SequenceNumber,
			})
			continue
		}
		if v.CounterID == counter.ID && v.CounterType == counter.CounterType {
			if err := app.cfg.ValidateVersion(v.CounterVersion); err != nil {
				log.Printf("[warn] validate viersion failed, maybe intermediate producer and this app version mismatch: %s", err)
			}
			records = append(records, &v)
		}
	}
	switch counter.CounterType {
	case Count:
		for _, record := range records {
			if record == nil {
				continue
			}
			state.RowCount += record.State.RowCount
		}
	case ApproxCountDistinct:
		hllpp, err := decodeBase64HLLPP(state.Base64HLLPP)
		if err != nil {
			return nil, err
		}
		for _, record := range records {
			otherHLLPP, err := decodeBase64HLLPP(record.State.Base64HLLPP)
			if err != nil {
				log.Printf("[warn] record HLLPP decode failed this record skip: %s", err)
				continue
			}
			if err := hllpp.Merge(otherHLLPP); err != nil {
				log.Printf("[warn] record HLLPP merge failed this record skip: %s", err)
				continue
			}
		}
		state.Base64HLLPP, err = encodeBase64HLLPP(hllpp)
		if err != nil {
			return nil, err
		}
	default:
		log.Printf("[debug] final invoke counter=%s", counter.ID)
		return nil, fmt.Errorf("unknown counter_type=%d", counter.CounterType)
	}
	resp.State[counter.ID] = app.setState(counter, event, state)
	if event.IsFinalInvokeForWindow {
		log.Printf("[debug] final invoke in aggregate counter=%s", counter.ID)
		if err := app.putStateRecord(ctx, counter, state, event); err != nil {
			return nil, err
		}
	}
	return resp, nil
}

func (app *App) putStateRecord(ctx context.Context, counter *CounterConfig, state *CounterState, event *KinesisTimeWindowEvent) error {
	log.Printf("[debug] put state record window %s", event.Window)
	v := map[string]interface{}{
		"event_source_arn": event.EventSourceArn,
		"window_start":     event.Window.Start.UnixMilli(),
		"window_end":       event.Window.End.UnixMilli(),
		"counter_id":       counter.ID,
		"counter_type":     counter.CounterType.String(),
	}
	if event.ShardID != "" {
		v["shard_id"] = event.ShardID
	}
	switch counter.CounterType {
	case Count:
		v["value"] = state.RowCount
	case ApproxCountDistinct:
		hllpp, err := decodeBase64HLLPP(state.Base64HLLPP)
		if err != nil {
			return err
		}
		v["value"] = hllpp.Count()
	default:
		log.Printf("[debug] final invoke counter=%s", counter.ID)
		return fmt.Errorf("unknown counter_type=%d", counter.CounterType)
	}
	if counter.transformer != nil {
		log.Printf("[debug] with transformer `%s`", counter.transformer.String())
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
				log.Printf("[error] failed marshal output record with jq_expr: %s", err)
				return err
			}
			if app.output != nil {
				if _, err := app.output.Write(bs); err != nil {
					return err
				}
				io.WriteString(app.output, "\n")
			}
			if err := app.putRecord(ctx, counter.OutputStreamARN, counter.ID, bs); err != nil {
				return err
			}
		}
		return nil
	}

	log.Println("[debug] without transformer")
	bs, err := json.Marshal(v)
	if err != nil {
		log.Printf("[error] failed marshal output record: %s", err)
		return err
	}
	if app.output != nil {
		if _, err := app.output.Write(bs); err != nil {
			return err
		}
		io.WriteString(app.output, "\n")
	}
	return app.putRecord(ctx, counter.OutputStreamARN, counter.ID, bs)
}

type IntermediateRecord struct {
	EventSourceARN string             `json:"event_source_arn,omitempty"`
	ShardID        string             `json:"shard_id,omitempty"`
	CounterID      string             `json:"counter_id,omitempty"`
	CounterType    CounterType        `json:"counter_type,omitempty"`
	CounterVersion string             `json:"counter_version,omitempty"`
	Window         *KinesisTimeWindow `json:"window,omitempty"`
	State          *CounterState      `json:"counter_state,omitempty"`
}

func (app *App) putIntermediateRecord(ctx context.Context, counter *CounterConfig, state *CounterState, event *KinesisTimeWindowEvent) error {
	v := &IntermediateRecord{
		EventSourceARN: event.EventSourceArn,
		ShardID:        event.ShardID,
		CounterID:      counter.ID,
		CounterType:    counter.CounterType,
		CounterVersion: app.version,
		Window:         event.Window,
		State:          state,
	}
	if app.aggregateChannel != nil {
		app.aggregateChannel <- *v
	}
	bs, err := json.Marshal(v)
	if err != nil {
		log.Printf("[error] failed marshal intermediate record: %s", err)
		return err
	}
	return app.putRecord(ctx, counter.AggregateStreamArn, counter.ID, bs)
}

func (app *App) putRecord(ctx context.Context, destinationARN *ARN, partitionKey string, data []byte) error {
	log.Println("[debug] start put record")
	if destinationARN == nil {
		log.Printf("[debug] put record arn is not set counter_id=%s", partitionKey)
		return nil
	}
	if app.ignorePutRecord {
		log.Println("[debug] ignore put record")
		return nil
	}
	log.Println("[info]", string(data))
	switch destinationARN.Service {
	case "kinesis":
		log.Println("[debug] put record to kinesis data stream")
		output, err := app.kinesisClient.PutRecord(ctx, &kinesis.PutRecordInput{
			Data:         data,
			PartitionKey: &partitionKey,
			StreamName:   aws.String(destinationARN.StreamName()),
		})
		if err != nil {
			log.Printf("[error] failed put record to kinesis data stream: %s", err)
			return err
		}
		log.Printf("[info] put record kinesis data stream=%s sequence_number=%s shard_id=%s", destinationARN.StreamName(), *output.SequenceNumber, *output.ShardId)
	case "firehose":
		log.Println("[debug] put record to kinesis data firehose")
		output, err := app.firehoseClient.PutRecord(ctx, &firehose.PutRecordInput{
			DeliveryStreamName: aws.String(destinationARN.StreamName()),
			Record: &firehosetypes.Record{
				Data: data,
			},
		})
		if err != nil {
			log.Printf("[error] failed put record to kinesis data firehose: %s", err)
			return err
		}
		log.Printf("[info] put record firehose delivery stream=%s record_id=%s", destinationARN.StreamName(), *output.RecordId)
	default:
		log.Printf("[warn] unexpected Service=%s, ARN=%s", destinationARN.Service, destinationARN.String())
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
