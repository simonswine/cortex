package querier

import (
	"context"
	"sort"

	"github.com/pkg/errors"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/prometheus/prometheus/scrape"
	"github.com/weaveworks/common/user"

	"github.com/cortexproject/cortex/pkg/ingester/client"
	"github.com/cortexproject/cortex/pkg/prom1/storage/metric"
)

var _ Distributor = &mergeDistributor{}

const tenantLabel model.LabelName = "__instance__"

type ReadTenantsResolver interface {
	UserID(context.Context) (string, error)
	ResolveReadTenants(context.Context) ([]string, string, error)
}

type hackyTenantResolver struct {
}

func (d *hackyTenantResolver) UserID(ctx context.Context) (string, error) {
	return user.ExtractOrgID(ctx)
}

func (d *hackyTenantResolver) ResolveReadTenants(ctx context.Context) ([]string, string, error) {
	userID, err := d.UserID(ctx)
	readTenantIDs := []string{userID}
	if userID == "user-c" {
		readTenantIDs = append(readTenantIDs, "user-a", "user-b")
	}
	return readTenantIDs, userID, err
}

type mergeDistributor struct {
	upstream Distributor
	resolver ReadTenantsResolver
}

func (d *mergeDistributor) Query(ctx context.Context, from, to model.Time, matchers ...*labels.Matcher) (model.Matrix, error) {
	readTenantIds, _, err := d.resolver.ResolveReadTenants(ctx)
	if err != nil {
		return nil, err
	}

	if len(readTenantIds) <= 1 {
		return d.upstream.Query(ctx, from, to, matchers...)
	}

	return nil, errors.New("Query merging not implemented")
}

func filterValuesByMatchers(key string, values []string, matchers ...*labels.Matcher) []string {
	found := false
	for _, m := range matchers {
		if m.Name != key {
			continue
		}
		found = true
	}

	if !found {
		return values
	}
}

func (d *mergeDistributor) QueryStream(ctx context.Context, from, to model.Time, matchers ...*labels.Matcher) (*client.QueryStreamResponse, error) {
	allReadTenantIDs, _, err := d.resolver.ResolveReadTenants(ctx)
	if err != nil {
		return nil, err
	}

	var readTenantIds []string
	for _, tenantID := range allReadTenantIDs {
	}

	if len(readTenantIds) <= 1 {
		return d.upstream.QueryStream(ctx, from, to, matchers...)
	}

	// TODO: handle matchers on tenantLabel

	var resp = &client.QueryStreamResponse{}
	for _, tenantID := range readTenantIds {
		tenantResp, err := d.upstream.QueryStream(
			user.InjectOrgID(ctx, tenantID),
			from,
			to,
			matchers...,
		)
		if err != nil {
			return nil, err
		}

		for _, ts := range tenantResp.Timeseries {
			ts.Labels = append(ts.Labels, client.LabelAdapter{
				Name:  string(tenantLabel),
				Value: tenantID,
			})
			resp.Timeseries = append(resp.Timeseries, ts)
		}

		for _, c := range tenantResp.Chunkseries {
			c.Labels = append(c.Labels, client.LabelAdapter{
				Name:  string(tenantLabel),
				Value: tenantID,
			})
			resp.Chunkseries = append(resp.Chunkseries, c)
		}
	}

	return resp, nil
}

func (d *mergeDistributor) LabelValuesForLabelName(ctx context.Context, labelName model.LabelName) ([]string, error) {
	readTenantIds, _, err := d.resolver.ResolveReadTenants(ctx)
	if err != nil {
		return nil, err
	}

	if len(readTenantIds) <= 1 {
		return d.upstream.LabelValuesForLabelName(ctx, labelName)
	}

	if labelName == tenantLabel {
		return readTenantIds, nil
	}

	funcs := []func() ([]string, error){
		// add tenant label label
		func() ([]string, error) { return []string{string(tenantLabel)}, nil },
	}
	for _, tenantID := range readTenantIds {
		funcs = append(
			funcs,
			func() ([]string, error) {
				return d.upstream.LabelValuesForLabelName(
					user.InjectOrgID(ctx, tenantID),
					labelName,
				)
			},
		)
	}
	return mergeDistinctStringSlice(funcs...)
}

func (d *mergeDistributor) LabelNames(ctx context.Context) ([]string, error) {
	readTenantIds, _, err := d.resolver.ResolveReadTenants(ctx)
	if err != nil {
		return nil, err
	}

	if len(readTenantIds) <= 1 {
		return d.upstream.LabelNames(ctx)
	}

	funcs := []func() ([]string, error){
		// add tenant label label
		func() ([]string, error) { return []string{string(tenantLabel)}, nil },
	}
	for _, tenantID := range readTenantIds {
		funcs = append(
			funcs,
			func() ([]string, error) {
				return d.upstream.LabelNames(
					user.InjectOrgID(ctx, tenantID),
				)
			},
		)
	}
	return mergeDistinctStringSlice(funcs...)
}

func (d *mergeDistributor) MetricsForLabelMatchers(ctx context.Context, from, through model.Time, matchers ...*labels.Matcher) ([]metric.Metric, error) {
	return d.upstream.MetricsForLabelMatchers(ctx, from, through, matchers...)
}

func (d *mergeDistributor) MetricsMetadata(ctx context.Context) ([]scrape.MetricMetadata, error) {
	// TODO: Implement me for readTenantIDs > 1
	// This can get interesting with mismatching metadata per server
	return d.upstream.MetricsMetadata(ctx)
}

func mergeDistinctStringSlice(funcs ...func() ([]string, error)) ([]string, error) {
	if len(funcs) == 1 {
		return funcs[0]()
	}

	resultMap := make(map[string]struct{})
	for _, f := range funcs {
		result, err := f()
		if err != nil {
			return nil, err
		}
		for _, e := range result {
			resultMap[e] = struct{}{}
		}
	}

	var result []string
	for e := range resultMap {
		result = append(result, e)
	}
	sort.Strings(result)
	return result, nil
}
