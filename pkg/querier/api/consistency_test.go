// SPDX-License-Identifier: AGPL-3.0-only

package api

import (
	"context"
	"fmt"
	"math"
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

var (
	// The following fixture has been generated looking at the actual metadata received by Grafana Mimir.
	exampleIncomingMetadata = metadata.New(map[string]string{
		"authority":            "1.1.1.1",
		"content-type":         "application/grpc",
		"grpc-accept-encoding": "snappy,gzip",
		"uber-trace-id":        "xxx",
		"user-agent":           "grpc-go/1.61.1",
		"x-scope-orgid":        "user-1",
	})
)

func BenchmarkReadConsistencyServerUnaryInterceptor(b *testing.B) {
	for _, withReadConsistency := range []bool{true, false} {
		b.Run(fmt.Sprintf("with read consistency: %t", withReadConsistency), func(b *testing.B) {
			md := exampleIncomingMetadata
			if withReadConsistency {
				md = metadata.Join(md, metadata.New(map[string]string{consistencyLevelGrpcMdKey: ReadConsistencyStrong}))
			}

			ctx := metadata.NewIncomingContext(context.Background(), md)

			for n := 0; n < b.N; n++ {
				_, _ = ReadConsistencyServerUnaryInterceptor(ctx, nil, nil, func(context.Context, any) (any, error) {
					return nil, nil
				})
			}
		})
	}
}

func BenchmarkReadConsistencyServerStreamInterceptor(b *testing.B) {
	for _, withReadConsistency := range []bool{true, false} {
		b.Run(fmt.Sprintf("with read consistency: %t", withReadConsistency), func(b *testing.B) {
			md := exampleIncomingMetadata
			if withReadConsistency {
				md = metadata.Join(md, metadata.New(map[string]string{consistencyLevelGrpcMdKey: ReadConsistencyStrong}))
			}

			stream := serverStreamMock{ctx: metadata.NewIncomingContext(context.Background(), md)}

			for n := 0; n < b.N; n++ {
				_ = ReadConsistencyServerStreamInterceptor(nil, stream, nil, func(_ any, _ grpc.ServerStream) error {
					return nil
				})
			}
		})
	}
}

type serverStreamMock struct {
	grpc.ServerStream

	ctx context.Context
}

func (m serverStreamMock) Context() context.Context {
	return m.ctx
}

func TestSerializedPartitionsOffset_Lookup(t *testing.T) {
	tests := map[string]struct {
		encoded              string
		expectedPartitions   map[int32]int64
		unexpectedPartitions []int32
	}{
		"empty": {
			encoded:              "",
			unexpectedPartitions: []int32{0},
		},
		"corruption when reading the partition ID": {
			encoded:              "x",
			unexpectedPartitions: []int32{0},
		},
		"corruption when reading the offset": {
			encoded:              "1:x",
			unexpectedPartitions: []int32{0},
		},
		"single partition": {
			encoded: PartitionsOffset{
				0: 123,
			}.Encode(),
			expectedPartitions:   map[int32]int64{0: 123},
			unexpectedPartitions: []int32{1},
		},
		"single partition with negative offset": {
			encoded: PartitionsOffset{
				0: -123,
			}.Encode(),
			expectedPartitions:   map[int32]int64{0: -123},
			unexpectedPartitions: []int32{1},
		},
		"multiple partitions": {
			encoded: PartitionsOffset{
				0: -123,
				1: 456,
				2: math.MaxInt64,
			}.Encode(),
			expectedPartitions:   map[int32]int64{0: -123, 1: 456, 2: math.MaxInt64},
			unexpectedPartitions: []int32{3},
		},
		"multiple partitions where partition IDs are in reverse order": {
			encoded:              "10:100,1:10",
			expectedPartitions:   map[int32]int64{1: 10, 10: 100},
			unexpectedPartitions: []int32{0, 100},
		},
	}

	for testName, testData := range tests {
		t.Run(testName, func(t *testing.T) {
			serialized := SerializedPartitionsOffset(testData.encoded)

			for expectedPartitionID, expectedOffset := range testData.expectedPartitions {
				actualOffset, ok := serialized.Lookup(expectedPartitionID)
				require.True(t, ok)
				assert.Equalf(t, expectedOffset, actualOffset, "partition ID: %d", expectedPartitionID)
			}

			for _, unexpectedPartitionID := range testData.unexpectedPartitions {
				_, ok := serialized.Lookup(unexpectedPartitionID)
				assert.Falsef(t, ok, "partition ID: %d", unexpectedPartitionID)
			}
		})
	}
}

func TestDigits(t *testing.T) {
	assert.Equal(t, 1, digits[int64](0))
	assert.Equal(t, 1, digits[int64](1))
	assert.Equal(t, 2, digits[int64](-1))
	assert.Equal(t, 2, digits[int64](10))
	assert.Equal(t, 3, digits[int64](-10))
	assert.Equal(t, len(strconv.Itoa(math.MaxInt64)), digits[int64](math.MaxInt64))
	// TODO assert.Equal(t, len(strconv.Itoa(math.MinInt64)), digits[int64](math.MinInt64))
}
