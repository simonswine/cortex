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
	readTenantIDs, _, err := d.resolver.ResolveReadTenants(ctx)
	if err != nil {
		return nil, err
	}

	if len(readTenantIDs) <= 1 {
		return d.upstream.Query(ctx, from, to, matchers...)
	}

	return nil, errors.New("Query merging not implemented")
}

func (d *mergeDistributor) QueryStream(ctx context.Context, from, to model.Time, matchers ...*labels.Matcher) (*client.QueryStreamResponse, error) {
	readTenantIDs, _, err := d.resolver.ResolveReadTenants(ctx)
	if err != nil {
		return nil, err
	}

	if len(readTenantIDs) <= 1 {
		return d.upstream.QueryStream(ctx, from, to, matchers...)
	}

	// handle matchers on tenantLabel
	readTenantIDs, matchers = filterValuesByMatchers(string(tenantLabel), readTenantIDs, matchers...)

	var resp = &client.QueryStreamResponse{}
	for _, tenantID := range readTenantIDs {
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
			ts.Labels = addOrUpdateLabels(ts.Labels, client.LabelAdapter{
				Name:  string(tenantLabel),
				Value: tenantID,
			})
			resp.Timeseries = append(resp.Timeseries, ts)
		}

		for _, c := range tenantResp.Chunkseries {
			c.Labels = addOrUpdateLabels(c.Labels, client.LabelAdapter{
				Name:  string(tenantLabel),
				Value: tenantID,
			})
			resp.Chunkseries = append(resp.Chunkseries, c)
		}
	}

	return resp, nil
}

func (d *mergeDistributor) LabelValuesForLabelName(ctx context.Context, labelName model.LabelName) ([]string, error) {
	readTenantIDs, _, err := d.resolver.ResolveReadTenants(ctx)
	if err != nil {
		return nil, err
	}

	if len(readTenantIDs) <= 1 {
		return d.upstream.LabelValuesForLabelName(ctx, labelName)
	}

	if labelName == tenantLabel {
		return readTenantIDs, nil
	}

	funcs := []func() ([]string, error){
		// add tenant label label
		func() ([]string, error) { return []string{string(tenantLabel)}, nil },
	}
	for _, tenantID := range readTenantIDs {
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
	readTenantIDs, _, err := d.resolver.ResolveReadTenants(ctx)
	if err != nil {
		return nil, err
	}

	if len(readTenantIDs) <= 1 {
		return d.upstream.LabelNames(ctx)
	}

	funcs := []func() ([]string, error){
		// add tenant label label
		func() ([]string, error) { return []string{string(tenantLabel)}, nil },
	}
	for _, tenantID := range readTenantIDs {
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

func addOrUpdateLabels(src []client.LabelAdapter, additionalLabels ...client.LabelAdapter) []client.LabelAdapter {
	i, j, result := 0, 0, make([]client.LabelAdapter, 0, len(src)+len(additionalLabels))
	for i < len(src) && j < len(additionalLabels) {
		if src[i].Name < additionalLabels[j].Name {
			result = append(result, client.LabelAdapter{
				Name:  src[i].Name,
				Value: src[i].Value,
			})
			i++
		} else if src[i].Name > additionalLabels[j].Name {
			result = append(result, additionalLabels[j])
			j++
		} else {
			result = append(result, additionalLabels[j])
			i++
			j++
		}
	}
	for ; i < len(src); i++ {
		result = append(result, client.LabelAdapter{
			Name:  src[i].Name,
			Value: src[i].Value,
		})
	}
	result = append(result, additionalLabels[j:]...)
	return result
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

func filterValuesByMatchers(labelName string, labelValues []string, matchers ...*labels.Matcher) ([]string, []*labels.Matcher) {
	// this contains the matchers which are not related to labelName
	var unrelatedMatchers []*labels.Matcher

	// this contains labelValues that are matched by the matchers
	var matchedLabelValues = labelValues

	for _, m := range matchers {
		if m.Name != labelName {
			unrelatedMatchers = append(unrelatedMatchers, m)
			continue
		}

		var matchThisMatcher []string
		for _, v := range matchedLabelValues {
			if m.Matches(v) {
				matchThisMatcher = append(matchThisMatcher, v)
			}
		}
		matchedLabelValues = matchThisMatcher
	}
	return matchedLabelValues, unrelatedMatchers
}
