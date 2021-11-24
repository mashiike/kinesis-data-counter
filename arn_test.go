package kinesisdatacounter_test

import (
	"fmt"
	"testing"

	kinesisdatacounter "github.com/mashiike/kinesis-data-counter"
	"github.com/stretchr/testify/require"
)

func TestARNMatch(t *testing.T) {
	cases := []struct {
		base     string
		other    string
		excepted bool
	}{
		{
			base:     "arn:aws:kinesis:ap-northeast-1:111122223333:stream/input-stream",
			other:    "arn:aws:kinesis:ap-northeast-1:111122223333:stream/input-stream",
			excepted: true,
		},
		{
			base:     "arn:aws:kinesis:ap-northeast-1:111122223333:stream/input-stream",
			other:    "arn:aws:kinesis:ap-northeast-1:111122223333:stream/output-stream",
			excepted: false,
		},
		{
			base:     "arn:aws:kinesis:*:*:stream/input-stream",
			other:    "arn:aws:kinesis:ap-northeast-1:111122223333:stream/input-stream",
			excepted: true,
		},
		{
			base:     "arn:aws:kinesis:*:*:stream/input-stream",
			other:    "arn:aws:kinesis:ap-northeast-1:111122223333:stream/output-stream",
			excepted: false,
		},
		{
			base:     "arn:aws:kinesis:*:*:stream/*",
			other:    "arn:aws:kinesis:ap-northeast-1:111122223333:stream/input-stream",
			excepted: true,
		},
		{
			base:     "arn:aws:*:ap-northeast-1:111122223333:*/hoge",
			other:    "arn:aws:firehose:ap-northeast-1:111122223333:deliverystream/hoge",
			excepted: true,
		},
		{
			base:     "arn:aws:*:*:*:*/*",
			other:    "arn:aws:firehose:ap-northeast-1:111122223333:deliverystream/hoge",
			excepted: true,
		},
		{
			base:     "*",
			other:    "arn:aws:firehose:ap-northeast-1:111122223333:deliverystream/hoge",
			excepted: true,
		},
	}
	for i, c := range cases {
		t.Run(fmt.Sprintf("case%d_%s", i, c.base), func(t *testing.T) {
			baseARN := &kinesisdatacounter.ARN{}
			err := baseARN.Set(c.base)
			require.NoError(t, err, "base must no error")
			actual := baseARN.Match(c.other)
			require.Equal(t, c.excepted, actual)
		})
	}
}

func TestARNStreamName(t *testing.T) {
	cases := []struct {
		base     string
		excepted string
	}{
		{
			base:     "arn:aws:firehose:ap-northeast-1:111122223333:deliverystream/output-stream-system",
			excepted: "output-stream-system",
		},
		{
			base:     "arn:aws:kinesis:ap-northeast-1:111122223333:stream/output-stream",
			excepted: "output-stream",
		},
	}
	for i, c := range cases {
		t.Run(fmt.Sprintf("case%d_%s", i, c.base), func(t *testing.T) {
			baseARN := &kinesisdatacounter.ARN{}
			err := baseARN.Set(c.base)
			require.NoError(t, err, "base must no error")
			actual := baseARN.StreamName()
			require.Equal(t, c.excepted, actual)
		})
	}
}
