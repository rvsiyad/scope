package tsdb

import (
	"fmt"
	"reflect"
	"testing"
)

func TestLabelsKeyCanonical(t *testing.T) {
	a := NewLabels("http_requests", map[string]string{"tenant": "acme", "model": "m1"})
	b := NewLabels("http_requests", map[string]string{"model": "m1", "tenant": "acme"})
	if a.Key() != b.Key() {
		t.Fatalf("map order changed identity: %q vs %q", a.Key(), b.Key())
	}
	if want := `http_requests{model="m1",tenant="acme"}`; a.Key() != want {
		t.Fatalf("key = %q, want %q", a.Key(), want)
	}
}

func TestLabelsKeyEscapesHostileValues(t *testing.T) {
	// Two different label sets that would collide under naive
	// concatenation must produce different keys.
	a := NewLabels("m", map[string]string{"a": `x",b="y`})
	b := NewLabels("m", map[string]string{"a": "x", "b": "y"})
	if a.Key() == b.Key() {
		t.Fatalf("hostile label value collided identities: %q", a.Key())
	}
}

func TestMatcherAbsentLabelIsEmpty(t *testing.T) {
	ls := NewLabels("m", map[string]string{"tenant": "acme"})
	if !Neq("region", "eu").Matches(ls) {
		t.Fatal(`region!="eu" must match a series with no region label`)
	}
	if !Eq("region", "").Matches(ls) {
		t.Fatal(`region="" must match a series with no region label`)
	}
}

func buildIndex(t *testing.T) *memIndex {
	t.Helper()
	ix := newMemIndex()
	// 3 tenants x 2 outcomes + one unrelated metric.
	for _, tenant := range []string{"acme", "globex", "initech"} {
		for _, outcome := range []string{"ok", "rejected"} {
			ls := NewLabels("gateway_requests_total",
				map[string]string{"tenant": tenant, "outcome": outcome})
			if _, created := ix.getOrCreate(ls); !created {
				t.Fatal("first sight must create")
			}
		}
	}
	ix.getOrCreate(NewLabels("gateway_ttft_ms", map[string]string{"tenant": "acme"}))
	return ix
}

func keysOf(ix *memIndex, ids []uint64) []string {
	out := make([]string, 0, len(ids))
	for _, id := range ids {
		out = append(out, ix.ids[id].Key())
	}
	return out
}

func TestGetOrCreateIsIdempotent(t *testing.T) {
	ix := buildIndex(t)
	n := ix.numSeries()
	ls := NewLabels("gateway_requests_total", map[string]string{"tenant": "acme", "outcome": "ok"})
	id1, created := ix.getOrCreate(ls)
	if created || ix.numSeries() != n {
		t.Fatal("re-registering an existing series must not create")
	}
	id2, _ := ix.getOrCreate(ls)
	if id1 != id2 {
		t.Fatal("same labels must resolve to the same ID")
	}
}

func TestSelectByIntersection(t *testing.T) {
	ix := buildIndex(t)
	cases := []struct {
		name     string
		matchers []Matcher
		want     int
	}{
		{"name only", []Matcher{Eq(MetricName, "gateway_requests_total")}, 6},
		{"name+tenant", []Matcher{Eq(MetricName, "gateway_requests_total"), Eq("tenant", "acme")}, 2},
		{"three-way", []Matcher{Eq(MetricName, "gateway_requests_total"), Eq("tenant", "acme"), Eq("outcome", "ok")}, 1},
		{"tenant across metrics", []Matcher{Eq("tenant", "acme")}, 3},
		{"unknown value", []Matcher{Eq("tenant", "hooli")}, 0},
		{"unknown label", []Matcher{Eq("region", "eu")}, 0},
		{"neq filter", []Matcher{Eq(MetricName, "gateway_requests_total"), Neq("outcome", "rejected")}, 3},
		{"neq only (scan)", []Matcher{Neq("outcome", "rejected")}, 4},
		{"no matchers (all)", nil, 7},
	}
	for _, tc := range cases {
		got := ix.selectIDs(tc.matchers)
		if len(got) != tc.want {
			t.Errorf("%s: got %d series %v, want %d", tc.name, len(got), keysOf(ix, got), tc.want)
		}
		// Results must always be ascending — merges downstream rely on it.
		for i := 1; i < len(got); i++ {
			if got[i] <= got[i-1] {
				t.Errorf("%s: ids not ascending: %v", tc.name, got)
			}
		}
	}
}

func TestSelectSpecificSeries(t *testing.T) {
	ix := buildIndex(t)
	got := ix.selectIDs([]Matcher{
		Eq(MetricName, "gateway_requests_total"), Eq("tenant", "globex"), Eq("outcome", "ok"),
	})
	want := []string{`gateway_requests_total{outcome="ok",tenant="globex"}`}
	if !reflect.DeepEqual(keysOf(ix, got), want) {
		t.Fatalf("got %v, want %v", keysOf(ix, got), want)
	}
}

func TestIntersect(t *testing.T) {
	cases := []struct{ a, b, want []uint64 }{
		{[]uint64{1, 2, 3}, []uint64{2, 3, 4}, []uint64{2, 3}},
		{[]uint64{1, 5, 9}, []uint64{2, 6, 10}, []uint64{}},
		{[]uint64{}, []uint64{1}, []uint64{}},
		{[]uint64{7}, []uint64{7}, []uint64{7}},
	}
	for _, tc := range cases {
		if got := intersect(tc.a, tc.b); !reflect.DeepEqual(got, tc.want) {
			t.Errorf("intersect(%v, %v) = %v, want %v", tc.a, tc.b, got, tc.want)
		}
	}
}

func TestPostingListsStayAscending(t *testing.T) {
	ix := newMemIndex()
	for i := 0; i < 100; i++ {
		ix.getOrCreate(NewLabels("m", map[string]string{"i": fmt.Sprint(i), "shared": "yes"}))
	}
	list := ix.postings[Label{"shared", "yes"}]
	if len(list) != 100 {
		t.Fatalf("shared posting list has %d entries, want 100", len(list))
	}
	for i := 1; i < len(list); i++ {
		if list[i] <= list[i-1] {
			t.Fatal("posting list not ascending")
		}
	}
}
