// Package tsdb is the storage engine: Prometheus's architecture,
// miniaturized. Samples land in an in-memory head block (one Gorilla
// stream per series), the head periodically flushes to immutable
// time-partitioned segment files, and an inverted label index — the same
// data structure as a search engine's — answers "which series match these
// labels" without scanning anything.
package tsdb

import (
	"fmt"
	"sort"
	"strings"
)

// MetricName is the label that holds the metric name. Making the name an
// ordinary label (Prometheus's convention) means one identity mechanism —
// the sorted label set — covers everything, and the index needs no special
// case for names.
const MetricName = "__name__"

// Label is one name/value pair.
type Label struct {
	Name  string `json:"name"`
	Value string `json:"value"`
}

// Labels is a series identity: label pairs sorted by name, unique names.
// Two series are the same series iff their Labels are equal — the TSDB has
// no other notion of identity.
type Labels []Label

// NewLabels builds a sorted Labels from a metric name and a label map.
func NewLabels(name string, labelMap map[string]string) Labels {
	ls := make(Labels, 0, len(labelMap)+1)
	ls = append(ls, Label{MetricName, name})
	for k, v := range labelMap {
		ls = append(ls, Label{k, v})
	}
	sort.Slice(ls, func(i, j int) bool { return ls[i].Name < ls[j].Name })
	return ls
}

// Get returns the value for a label name, or "".
func (ls Labels) Get(name string) string {
	for _, l := range ls {
		if l.Name == name {
			return l.Value
		}
	}
	return ""
}

// Key is the canonical string form used as a map key and for display:
// name{k="v",...} with the __name__ label lifted out front. Values are
// %q-escaped so adversarial label values can't collide two identities.
func (ls Labels) Key() string {
	var sb strings.Builder
	sb.WriteString(ls.Get(MetricName))
	sb.WriteByte('{')
	first := true
	for _, l := range ls {
		if l.Name == MetricName {
			continue
		}
		if !first {
			sb.WriteByte(',')
		}
		first = false
		fmt.Fprintf(&sb, "%s=%q", l.Name, l.Value)
	}
	sb.WriteByte('}')
	return sb.String()
}

// MatchType is how a Matcher compares.
type MatchType int

const (
	MatchEq MatchType = iota
	MatchNeq
)

// Matcher is one label constraint, e.g. tenant="acme" or outcome!="ok".
type Matcher struct {
	Type  MatchType
	Name  string
	Value string
}

// Eq and Neq are convenience constructors for the query layer and tests.
func Eq(name, value string) Matcher  { return Matcher{MatchEq, name, value} }
func Neq(name, value string) Matcher { return Matcher{MatchNeq, name, value} }

// Matches reports whether one label set satisfies the matcher. An absent
// label matches as the empty string, mirroring PromQL: foo!="x" selects
// series without a foo label at all.
func (m Matcher) Matches(ls Labels) bool {
	v := ls.Get(m.Name)
	if m.Type == MatchEq {
		return v == m.Value
	}
	return v != m.Value
}
