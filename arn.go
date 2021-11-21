package kinesisdatacounter

import (
	"errors"
	"fmt"
	"log"
	"strings"

	awsarn "github.com/aws/aws-sdk-go-v2/aws/arn"
)

type ARN struct {
	awsarn.ARN
}

func (arn *ARN) Set(text string) error {
	if text == "*" {
		arn.ARN.Partition = "*"
		arn.ARN.AccountID = "*"
		arn.ARN.Region = "*"
		arn.ARN.Service = "*"
		arn.ARN.Resource = "*/*"
	}
	var err error
	arn.ARN, err = awsarn.Parse(text)
	if err != nil {
		return err
	}
	if arn.Partition != "aws" {
		return errors.New("ARN Partition is not aws")
	}
	if arn.Service == "kinesis" && strings.HasPrefix(arn.Resource, "stream/") {
		return nil
	}
	if arn.Service == "firehose" && strings.HasPrefix(arn.Resource, "deliverystream/") {
		return nil
	}
	if arn.Service == "*" && strings.HasPrefix(arn.Resource, "*/") {
		return nil
	}
	return fmt.Errorf("arn is not kinesis data stream or kinesis data firehose: %s", arn.ARN.String())
}

func (arn *ARN) UnmarshalText(text []byte) error {
	return arn.Set(string(text))
}

func (arn *ARN) MarshalText() ([]byte, error) {
	return []byte(arn.ARN.String()), nil
}

func (arn *ARN) IsAmbiguous() bool {
	return strings.ContainsRune(arn.String(), '*')
}

func (arn *ARN) IsKinesisDataStream() bool {
	return arn.Service == "kinesis"
}

func (arn *ARN) Match(other string) bool {
	if !arn.IsAmbiguous() {
		return arn.String() == other
	}
	otherARN, err := awsarn.Parse(other)
	if err != nil {
		log.Printf("[debug] can not parse as arn: string=%s err=%s", other, err)
		return false
	}
	if !matchPart(arn.Partition, otherARN.Partition) {
		return false
	}
	if !matchPart(arn.Region, otherARN.Region) {
		return false
	}
	if !matchPart(arn.Service, otherARN.Service) {
		return false
	}
	if !matchPart(arn.AccountID, otherARN.AccountID) {
		return false
	}
	if !matchPart(arn.Resource, otherARN.Resource) {
		return false
	}
	return true
}

func (arn *ARN) StreamName() string {
	parts := strings.SplitN(arn.Resource, arn.Resource, 2)
	return parts[1]
}

func matchPart(arnPart, otherPart string) bool {
	if !strings.ContainsRune(arnPart, '*') {
		return arnPart == otherPart
	}
	parts := strings.Split(arnPart, "*")
	index := 0
	for _, part := range parts {
		nextIndex := strings.Index(otherPart[index:], part)
		if nextIndex == -1 {
			return false
		}
		index = nextIndex + len(part)
	}
	return true
}
