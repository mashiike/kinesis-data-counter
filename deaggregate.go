package kinesisdatacounter

import (
	"context"

	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/kinesis"
	deaggregator "github.com/awslabs/kinesis-aggregation/go/deaggregator"
)

func (app *App) deaggregate(ctx context.Context, before []events.KinesisEventRecord) ([]events.KinesisEventRecord, error) {
	temp := make([]*kinesis.Record, 0, len(before))
	for _, record := range before {
		temp = append(temp, &kinesis.Record{
			ApproximateArrivalTimestamp: aws.Time(record.Kinesis.ApproximateArrivalTimestamp.UTC()),
			Data:                        record.Kinesis.Data,
			EncryptionType:              aws.String(record.Kinesis.EncryptionType),
			PartitionKey:                aws.String(record.Kinesis.EncryptionType),
			SequenceNumber:              aws.String(record.Kinesis.SequenceNumber),
		})
	}

	deaggregated, err := deaggregator.DeaggregateRecords(temp)
	if err != nil {
		return nil, err
	}
	after := make([]events.KinesisEventRecord, 0, len(deaggregated))
	for _, record := range deaggregated {
		after = append(after, events.KinesisEventRecord{
			Kinesis: events.KinesisRecord{
				ApproximateArrivalTimestamp: events.SecondsEpochTime{
					Time: *record.ApproximateArrivalTimestamp,
				},
				Data:           record.Data,
				EncryptionType: *record.EncryptionType,
				PartitionKey:   *record.PartitionKey,
				SequenceNumber: *record.SequenceNumber,
			},
		})
	}
	return after, nil
}
