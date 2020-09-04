package querier

import (
	"testing"

	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/stretchr/testify/assert"
)

func TestAddOrUpdateLabels(t *testing.T) {
	for _, tc := range []struct {
		labels           labels.Labels
		additionalLabels labels.Labels
		expected         labels.Labels
	}{
		// Test adding labels at the end.
		{
			labels:           labels.Labels{{Name: "a", Value: "b"}},
			additionalLabels: labels.Labels{{Name: "c", Value: "d"}},
			expected:         labels.Labels{{Name: "a", Value: "b"}, {Name: "c", Value: "d"}},
		},

		// Test adding labels at the beginning.
		{
			labels:           labels.Labels{{Name: "c", Value: "d"}},
			additionalLabels: labels.Labels{{Name: "a", Value: "b"}},
			expected:         labels.Labels{{Name: "a", Value: "b"}, {Name: "c", Value: "d"}},
		},

		// Test we do override existing labels.
		{
			labels:           labels.Labels{{Name: "a", Value: "b"}},
			additionalLabels: labels.Labels{{Name: "a", Value: "c"}},
			expected:         labels.Labels{{Name: "a", Value: "c"}},
		},
	} {
		assert.Equal(t, tc.expected, addOrUpdateLabels(tc.labels, tc.additionalLabels...))
	}
}
