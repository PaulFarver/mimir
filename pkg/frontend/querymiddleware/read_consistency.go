package querymiddleware

import (
	"net/http"

	"github.com/go-kit/log"
	"github.com/grafana/dskit/tenant"

	apierror "github.com/grafana/mimir/pkg/api/error"
	querierapi "github.com/grafana/mimir/pkg/querier/api"
	"github.com/grafana/mimir/pkg/storage/ingest"
	"github.com/grafana/mimir/pkg/util/spanlogger"
	"github.com/grafana/mimir/pkg/util/validation"
)

type readConsistencyRoundTripper struct {
	next http.RoundTripper

	offsetsReader *ingest.PartitionOffsetsReader
	limits        Limits
	logger        log.Logger
}

func newReadConsistencyRoundTripper(next http.RoundTripper, offsetsReader *ingest.PartitionOffsetsReader, limits Limits, logger log.Logger) http.RoundTripper {
	return &readConsistencyRoundTripper{
		next:          next,
		limits:        limits,
		logger:        logger,
		offsetsReader: offsetsReader,
	}
}

// TODO unit test
// TODO add a metric to track how long it takes
func (r *readConsistencyRoundTripper) RoundTrip(req *http.Request) (*http.Response, error) {
	ctx := req.Context()

	spanLog, ctx := spanlogger.NewWithLogger(ctx, r.logger, "readConsistencyRoundTripper.RoundTrip")
	defer spanLog.Finish()

	// Fetch the tenant ID(s).
	tenantIDs, err := tenant.TenantIDs(ctx)
	if err != nil {
		return nil, apierror.New(apierror.TypeBadData, err.Error())
	}

	// Detect the requested read consistency level.
	level, ok := querierapi.ReadConsistencyFromContext(req.Context())
	if !ok {
		// TODO this logic is also in the ingester. I think it's cleaner if we read it only in the query-frontend and then inject the updated consistency level in the context.
		level = validation.PreferredStringPerTenant(tenantIDs, r.limits.IngestStorageReadConsistency, []string{querierapi.ReadConsistencyStrong})
	}

	if level != querierapi.ReadConsistencyStrong {
		return r.next.RoundTrip(req)
	}

	// TODO DEBUG
	//fmt.Println("readConsistencyRoundTripper.RoundTrip() read consistency:", level)

	offsets, err := r.offsetsReader.WaitNextFetchLastProducedOffsets(ctx)
	if err != nil {
		// TODO wrap the error
		return nil, err
	}

	// Exclude all empty partitions. Since Kafka partitions may have been pre-provisioned and many of them
	// unused, we filter out all empty partitions.
	offsets = offsets.WithoutEmptyPartitions()

	// TODO DEBUG
	//fmt.Println("readConsistencyRoundTripper.RoundTrip() last produced offsets:", offsets)

	// Inject the last produced offsets in the context.
	//ctx = querierapi.ContextWithReadConsistencyOffsets(ctx, offsets)
	//req = req.WithContext(ctx)
	// TODO we don't have to encode it for each request. We can be smarter and "cache" the encoded version.
	req.Header.Add(querierapi.ReadConsistencyOffsetsHeader, offsets.Encode())

	return r.next.RoundTrip(req)
}
