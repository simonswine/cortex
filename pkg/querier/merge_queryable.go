package querier

import (
	"context"
	"fmt"
	"sort"
	"time"

	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
	tsdb_errors "github.com/prometheus/prometheus/tsdb/errors"
	"github.com/weaveworks/common/user"
)

const tenantLabel model.LabelName = "__instance__"

type MultiTenantResolver interface {
	// Return the UserID from the request context
	UserID(context.Context) (string, error)
	// Return the readTenantIDS
	ReadTenantIDs(context.Context) ([]string, string, error)
	TenantLabelName(context.Context) (model.LabelName, error)
}

var _ QueryableWithFilter = &mergeQueryable{}

type mergeQueryable struct {
	upstream QueryableWithFilter
	resolver MultiTenantResolver
}

type mergeQuerier struct {
	queriers        []storage.Querier
	tenantIDs       []string
	tenantLabelName model.LabelName
}

// LabelValues returns all potential values for a label name.
// It is not safe to use the strings beyond the lifefime of the querier.
func (m *mergeQuerier) LabelValues(name string) ([]string, storage.Warnings, error) {
	if name == string(m.tenantLabelName) {
		return m.tenantIDs, nil, nil
	}

	var funcs []func() ([]string, storage.Warnings, error)
	for pos := range m.tenantIDs {
		funcs = append(
			funcs,
			func() ([]string, storage.Warnings, error) {
				return m.queriers[pos].LabelValues(name)
			},
		)
	}
	return mergeDistinctStringSlice(funcs...)
}

// LabelNames returns all the unique label names present in the block in sorted order.
func (m *mergeQuerier) LabelNames() ([]string, storage.Warnings, error) {
	funcs := []func() ([]string, storage.Warnings, error){
		// add tenant label label
		func() ([]string, storage.Warnings, error) { return []string{string(m.tenantLabelName)}, nil, nil },
	}
	for pos := range m.tenantIDs {
		funcs = append(
			funcs,
			func() ([]string, storage.Warnings, error) {
				return m.queriers[pos].LabelNames()
			},
		)
	}
	return mergeDistinctStringSlice(funcs...)
}

// Close releases the resources of the Querier.
func (m *mergeQuerier) Close() error {
	var errs tsdb_errors.MultiError
	for pos := range m.tenantIDs {
		if err := m.queriers[pos].Close(); err != nil {
			errs = append(errs, fmt.Errorf("failed to close querier for %s: %w", m.tenantIDs, err))
		}
	}
	return errs.Err()
}

// Select returns a set of series that matches the given label matchers.
// Caller can specify if it requires returned series to be sorted. Prefer not requiring sorting for better performance.
// It allows passing hints that can help in optimising select, but it's up to implementation how this is used if used at all.
func (m *mergeQuerier) Select(sortSeries bool, hints *storage.SelectHints, matchers ...*labels.Matcher) storage.SeriesSet {
	tenantIDsPos, filteredMatchers := filterValuesByMatchers(string(m.tenantLabelName), m.tenantIDs, matchers...)
	var seriesSets = make([]storage.SeriesSet, len(tenantIDsPos))
	for pos, posTenant := range tenantIDsPos {
		tenantID := m.tenantIDs[posTenant]
		seriesSets[pos] = &addLabelsSeriesSet{
			upstream: m.queriers[posTenant].Select(sortSeries, hints, filteredMatchers...),
			labels: labels.Labels{
				{
					Name:  string(m.tenantLabelName),
					Value: tenantID,
				},
			},
		}
	}
	return storage.NewMergeSeriesSet(seriesSets, storage.ChainedSeriesMerge)
}

type addLabelsSeriesSet struct {
	upstream storage.SeriesSet
	labels   labels.Labels
}

func (m *addLabelsSeriesSet) Next() bool {
	return m.upstream.Next()
}

// At returns full series. Returned series should be iteratable even after Next is called.
func (m *addLabelsSeriesSet) At() storage.Series {
	return &addLabelsSeries{
		upstream: m.upstream.At(),
		labels:   m.labels,
	}
}

// The error that iteration as failed with.
// When an error occurs, set cannot continue to iterate.
func (m *addLabelsSeriesSet) Err() error {
	return m.upstream.Err()
}

// A collection of warnings for the whole set.
// Warnings could be return even iteration has not failed with error.
func (m *addLabelsSeriesSet) Warnings() storage.Warnings {
	return m.upstream.Warnings()
}

type addLabelsSeries struct {
	upstream storage.Series
	labels   labels.Labels
}

// Labels returns the complete set of labels. For series it means all labels identifying the series.
func (a *addLabelsSeries) Labels() labels.Labels {
	return addOrUpdateLabels(a.upstream.Labels(), a.labels...)
}

// Iterator returns a new, independent iterator of the data of the series.
func (a *addLabelsSeries) Iterator() chunkenc.Iterator {
	return a.upstream.Iterator()
}

func (m *mergeQueryable) UseQueryable(now time.Time, queryMinT, queryMaxT int64) bool {
	return m.upstream.UseQueryable(now, queryMinT, queryMaxT)
}

// Querier returns a new mergeQuerier aggregating all readTenants into the result
func (m *mergeQueryable) Querier(ctx context.Context, mint int64, maxt int64) (storage.Querier, error) {
	readTenantIDs, _, err := m.resolver.ReadTenantIDs(ctx)
	if err != nil {
		return nil, err
	}
	fmt.Printf("TODO::READ: %v\n", readTenantIDs)
	if len(readTenantIDs) <= 1 {
		return m.upstream.Querier(ctx, mint, maxt)
	}

	tenantLabelName, err := m.resolver.TenantLabelName(ctx)
	if err != nil {
		return nil, err
	}

	var queriers = make([]storage.Querier, len(readTenantIDs))
	for pos, tenantID := range readTenantIDs {
		q, err := m.upstream.Querier(
			user.InjectOrgID(ctx, tenantID),
			mint,
			maxt,
		)
		if err != nil {
			return nil, err
		}
		queriers[pos] = q
	}

	return &mergeQuerier{
		queriers:        queriers,
		tenantIDs:       readTenantIDs,
		tenantLabelName: tenantLabelName,
	}, nil
}

func addOrUpdateLabels(src labels.Labels, additionalLabels ...labels.Label) labels.Labels {
	i, j, result := 0, 0, make(labels.Labels, 0, len(src)+len(additionalLabels))
	for i < len(src) && j < len(additionalLabels) {
		if src[i].Name < additionalLabels[j].Name {
			result = append(result, labels.Label{
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
		result = append(result, labels.Label{
			Name:  src[i].Name,
			Value: src[i].Value,
		})
	}
	result = append(result, additionalLabels[j:]...)
	return result
}

func mergeDistinctStringSlice(funcs ...func() ([]string, storage.Warnings, error)) ([]string, storage.Warnings, error) {
	if len(funcs) == 1 {
		return funcs[0]()
	}

	var warnings storage.Warnings
	resultMap := make(map[string]struct{})
	for _, f := range funcs {
		result, w, err := f()
		if err != nil {
			return nil, nil, err
		}
		for _, e := range result {
			resultMap[e] = struct{}{}
		}
		warnings = append(warnings, w...)
	}

	var result []string
	for e := range resultMap {
		result = append(result, e)
	}
	sort.Strings(result)
	return result, warnings, nil
}

func filterValuesByMatchers(labelName string, labelValues []string, matchers ...*labels.Matcher) ([]int, []*labels.Matcher) {
	// this contains the matchers which are not related to labelName
	var unrelatedMatchers []*labels.Matcher

	// this contains the pos of labelValues that are matched by the matchers
	var matchedValuesPos = make([]int, len(labelValues))
	for pos := range labelValues {
		matchedValuesPos[pos] = pos
	}

	for _, m := range matchers {
		if m.Name != labelName {
			unrelatedMatchers = append(unrelatedMatchers, m)
			continue
		}

		var matchedValuesPosThisMatcher []int
		for _, v := range matchedValuesPos {
			if m.Matches(labelValues[v]) {
				matchedValuesPosThisMatcher = append(matchedValuesPosThisMatcher, v)
			}
		}
		matchedValuesPos = matchedValuesPosThisMatcher
	}
	return matchedValuesPos, unrelatedMatchers
}
