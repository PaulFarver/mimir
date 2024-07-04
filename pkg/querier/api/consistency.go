// SPDX-License-Identifier: AGPL-3.0-only

package api

import (
	"context"
	"math"
	"net/http"
	"slices"
	"strconv"
	"strings"

	"github.com/grafana/dskit/middleware"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

const (
	ReadConsistencyHeader        = "X-Read-Consistency"
	ReadConsistencyOffsetsHeader = "X-Read-Consistency-Offsets"

	// ReadConsistencyStrong means that a query sent by the same client will always observe the writes
	// that have completed before issuing the query.
	ReadConsistencyStrong = "strong"

	// ReadConsistencyEventual is the default consistency level for all queries.
	// This level means that a query sent by a client may not observe some of the writes that the same client has recently made.
	ReadConsistencyEventual = "eventual"
)

var ReadConsistencies = []string{ReadConsistencyStrong, ReadConsistencyEventual}

func IsValidReadConsistency(lvl string) bool {
	return slices.Contains(ReadConsistencies, lvl)
}

type contextKey int

const consistencyContextKey contextKey = 1
const producedOffsetsContextKey contextKey = 2

// ContextWithReadConsistency returns a new context with the given consistency level.
// The consistency level can be retrieved with ReadConsistencyFromContext.
// TODO rename to ContextWithReadConsistencyLevel
func ContextWithReadConsistency(parent context.Context, level string) context.Context {
	return context.WithValue(parent, consistencyContextKey, level)
}

// ReadConsistencyFromContext returns the consistency level from the context if set via ContextWithReadConsistency.
// The second return value is true if the consistency level was found in the context and is valid.
// TODO rename to ReadConsistencyLevelFromContext
func ReadConsistencyFromContext(ctx context.Context) (string, bool) {
	level, _ := ctx.Value(consistencyContextKey).(string)
	return level, IsValidReadConsistency(level)
}

// TODO doc + unit test
func ContextWithReadConsistencyOffsets(ctx context.Context, offsets PartitionsOffset) context.Context {
	return ContextWithReadConsistencyEncodedOffsets(ctx, offsets.Encode())
}

// TODO doc + unit test
func ContextWithReadConsistencyEncodedOffsets(ctx context.Context, offsets string) context.Context {
	return context.WithValue(ctx, producedOffsetsContextKey, offsets)
}

// TODO doc + unit test
// TODO work with an interface instead of SerializedPartitionsOffset (we just need Lookup())
func ReadConsistencyOffsetsFromContext(ctx context.Context) (SerializedPartitionsOffset, bool) {
	encoded, ok := ctx.Value(producedOffsetsContextKey).(string)
	if !ok {
		return "", false
	}

	return SerializedPartitionsOffset(encoded), true
}

// ConsistencyMiddleware takes the consistency level from the X-Read-Consistency header and sets it in the context.
// It can be retrieved with ReadConsistencyFromContext.
func ConsistencyMiddleware() middleware.Interface {
	return middleware.Func(func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if level := r.Header.Get(ReadConsistencyHeader); IsValidReadConsistency(level) {
				r = r.WithContext(ContextWithReadConsistency(r.Context(), level))
			}
			if offsets := r.Header.Get(ReadConsistencyOffsetsHeader); len(offsets) > 0 {
				r = r.WithContext(ContextWithReadConsistencyEncodedOffsets(r.Context(), offsets))
			}

			next.ServeHTTP(w, r)
		})
	})
}

const (
	consistencyLevelGrpcMdKey   = "__consistency_level__"
	consistencyOffsetsGrpcMdKey = "__consistency_offsets__"
)

func ReadConsistencyClientUnaryInterceptor(ctx context.Context, method string, req any, reply any, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
	if value, ok := ReadConsistencyFromContext(ctx); ok {
		ctx = metadata.AppendToOutgoingContext(ctx, consistencyLevelGrpcMdKey, value)
	}
	if value, ok := ReadConsistencyOffsetsFromContext(ctx); ok {
		// TODO test
		ctx = metadata.AppendToOutgoingContext(ctx, consistencyOffsetsGrpcMdKey, string(value))
	}
	return invoker(ctx, method, req, reply, cc, opts...)
}

func ReadConsistencyServerUnaryInterceptor(ctx context.Context, req interface{}, _ *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
	levels := metadata.ValueFromIncomingContext(ctx, consistencyLevelGrpcMdKey)
	if len(levels) > 0 && IsValidReadConsistency(levels[0]) {
		ctx = ContextWithReadConsistency(ctx, levels[0])
	}

	// TODO test
	offsets := metadata.ValueFromIncomingContext(ctx, consistencyOffsetsGrpcMdKey)
	if len(offsets) > 0 {
		ctx = ContextWithReadConsistencyEncodedOffsets(ctx, offsets[0])
	}

	return handler(ctx, req)
}

func ReadConsistencyClientStreamInterceptor(ctx context.Context, desc *grpc.StreamDesc, cc *grpc.ClientConn, method string, streamer grpc.Streamer, opts ...grpc.CallOption) (grpc.ClientStream, error) {
	if value, ok := ReadConsistencyFromContext(ctx); ok {
		ctx = metadata.AppendToOutgoingContext(ctx, consistencyLevelGrpcMdKey, value)
	}
	if value, ok := ReadConsistencyOffsetsFromContext(ctx); ok {
		// TODO test
		ctx = metadata.AppendToOutgoingContext(ctx, consistencyOffsetsGrpcMdKey, string(value))
	}
	return streamer(ctx, desc, cc, method, opts...)
}

func ReadConsistencyServerStreamInterceptor(srv interface{}, ss grpc.ServerStream, _ *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
	ctx := ss.Context()

	levels := metadata.ValueFromIncomingContext(ss.Context(), consistencyLevelGrpcMdKey)
	if len(levels) > 0 && IsValidReadConsistency(levels[0]) {
		ctx = ContextWithReadConsistency(ctx, levels[0])
	}

	// TODO test
	offsets := metadata.ValueFromIncomingContext(ctx, consistencyOffsetsGrpcMdKey)
	if len(offsets) > 0 {
		ctx = ContextWithReadConsistencyEncodedOffsets(ctx, offsets[0])
	}

	ss = ctxStream{
		ctx:          ctx,
		ServerStream: ss,
	}
	return handler(srv, ss)
}

type ctxStream struct {
	ctx context.Context
	grpc.ServerStream
}

func (ss ctxStream) Context() context.Context {
	return ss.ctx
}

// TODO doc
type PartitionsOffset map[int32]int64

//func (p PartitionsOffset) Encode() []byte {
//	encoded := make([]byte, 0, len(p)*10)
//
//	for partitionID, offset := range p {
//		encoded = binary.LittleEndian.AppendUint32(encoded, uint32(partitionID))
//		encoded = binary.LittleEndian.AppendUint64(encoded, uint64(offset))
//	}
//
//	return encoded
//}

// TODO unit test
func (p PartitionsOffset) WithoutEmptyPartitions() PartitionsOffset {
	filtered := make(PartitionsOffset, len(p))

	for partitionID, offset := range p {
		if offset < 0 {
			continue
		}

		filtered[partitionID] = offset
	}

	return filtered
}

// TODO unit test
// TODO benchmark
func (p PartitionsOffset) Encode() string {
	// Count the number of digits (eventually including the minus sign).
	size := 0
	for partitionID, offset := range p {
		sizeSeparator := 0
		if size > 0 {
			sizeSeparator = 1
		}

		size += digits(partitionID) + 1 + digits(offset) + sizeSeparator
	}

	// Encode the key-value pairs using ASCII characters, so that they can be safely included
	// in an HTTP header.
	builder := strings.Builder{}
	builder.Grow(size)

	count := 0
	for partitionID, offset := range p {
		count++

		builder.WriteString(strconv.FormatInt(int64(partitionID), 10))
		builder.WriteRune(':')
		builder.WriteString(strconv.FormatInt(offset, 10))

		// Add the separator, unless it's the last entry.
		if count < len(p) {
			builder.WriteRune(',')
		}
	}

	return builder.String()
}

func digits[T int32 | int64](value T) int {
	if value == 0 {
		return 1
	}

	// Handle negative numbers by considering the minus sign.
	num := 0
	if value < 0 {
		num++
		value = -value
	}

	num += int(math.Log10(float64(value))) + 1
	return num
}

type SerializedPartitionsOffset string

// TODO benchmark
// TODO improve unit tests
func (p SerializedPartitionsOffset) Lookup(partitionID int32) (int64, bool) {
	if len(p) == 0 {
		return 0, false
	}

	// Find the position of the partition.
	partitionKey := strconv.FormatInt(int64(partitionID), 10) + ":"
	partitionIdx := -1

	// TODO this is a terrible implementation. A simple sequence scan may be better.
	for start := 0; start < len(p); {
		idx := strings.Index(string(p[start:]), partitionKey)
		if idx < 0 {
			break
		}

		partitionIdx = idx + start

		idx = strings.LastIndex(string(p[:partitionIdx]), ",")
		if idx < 0 {
			partitionIdx = 0
		} else {
			partitionIdx = idx + 1
		}

		if actual := string(p[partitionIdx : partitionIdx+len(partitionKey)]); actual == partitionKey {
			// Found it.
			break
		}

		// We haven't found the exact match, so we keep searching.
		start = partitionIdx + len(partitionKey)
	}

	if partitionIdx < 0 {
		return 0, false
	}

	// Find the end index of the offset.
	offsetEndIdx := strings.Index(string(p[partitionIdx+len(partitionKey):]), ",")
	if offsetEndIdx >= 0 {
		offsetEndIdx += partitionIdx + len(partitionKey)
	} else {
		offsetEndIdx = len(p)
	}

	// Extract the offset.
	offset, err := strconv.ParseInt(string(p[partitionIdx+len(partitionKey):offsetEndIdx]), 10, 64)
	if err != nil {
		return 0, false
	}

	return offset, true
}

// TODO if we sort the partitions then we can do a binary search (actually we can guess the exact position given we typically expect sequential partitions
//func (p SerializedPartitionsOffset) Lookup(partitionID int32) (int64, bool) {
//	for idx := 0; idx < len(p); {
//		if len(p) < idx+4 {
//			return 0, false
//		}
//
//		currPartitionID := int32(binary.LittleEndian.Uint32(p[idx : idx+4]))
//		idx += 4
//
//		// Check if the partition matches. If not, then we skip the offset and move to the next entry.
//		if currPartitionID != partitionID {
//			idx += 8
//			continue
//		}
//
//		if len(p) < idx+8 {
//			return 0, false
//		}
//
//		return int64(binary.LittleEndian.Uint64(p[idx : idx+8])), true
//	}
//
//	return 0, false
//}
