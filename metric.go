package zdb

import (
	"context"
	"database/sql"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	"zgo.at/zdb/internal/sqlx"
	"zgo.at/zstd/ztime"
)

type MetricRecorder interface {
	Record(d time.Duration, query string, params []any)
}

// MetricsMemory records metrics in memory.
type MetricsMemory struct {
	mu      *sync.Mutex
	max     int
	metrics map[string]ztime.Durations
}

// NewMetricsMemory creates a new MetricsMemory, up to "max" metrics per query.
func NewMetricsMemory(max int) *MetricsMemory {
	return &MetricsMemory{
		mu:      new(sync.Mutex),
		max:     max,
		metrics: make(map[string]ztime.Durations),
	}
}

// Reset the contents.
func (m *MetricsMemory) Reset() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.metrics = make(map[string]ztime.Durations)
}

// Record this query.
func (m *MetricsMemory) Record(d time.Duration, query string, params []any) {
	m.mu.Lock()
	defer m.mu.Unlock()

	x, ok := m.metrics[query]
	if !ok {
		x = ztime.NewDurations(m.max)
	}
	x.Append(d)
	m.metrics[query] = x
}

// Queries gets a list of queries sorted by the total run time.
func (m *MetricsMemory) Queries() []struct {
	Query string
	Times ztime.Durations
} {
	m.mu.Lock()
	defer m.mu.Unlock()

	l := make([]struct {
		Query string
		Times ztime.Durations
	}, 0, len(m.metrics))

	for k, v := range m.metrics {
		l = append(l, struct {
			Query string
			Times ztime.Durations
		}{k, v})
	}

	sort.Slice(l, func(i, j int) bool {
		return l[i].Times.Sum() > l[j].Times.Sum()
	})

	return l
}

func (m *MetricsMemory) String() string {
	b := new(strings.Builder)
	for _, q := range m.Queries() {
		fmt.Fprintf(b, "Query %q:\n", q.Query)
		fmt.Fprintf(b, "    Run time:  %6s\n", q.Times.Sum())
		fmt.Fprintf(b, "    Min:       %6s\n", q.Times.Min())
		fmt.Fprintf(b, "    Max:       %6s\n", q.Times.Max())
		fmt.Fprintf(b, "    Median:    %6s\n", q.Times.Median())
		fmt.Fprintf(b, "    Mean:      %6s\n", q.Times.Mean())
	}
	return b.String()
}

type metricDB struct {
	DB
	recorder MetricRecorder
}

// NewMetricsDB returns a DB wrapper which records query performance metrics.
//
// For every query recorder.Record is called.
func NewMetricsDB(db DB, recorder MetricRecorder) DB {
	return &metricDB{DB: db, recorder: recorder}
}

func (d metricDB) Unwrap() DB { return d.DB }

func (d metricDB) Begin(ctx context.Context, opts ...beginOpt) (context.Context, DB, error) {
	ctx, tx, err := d.DB.Begin(ctx, opts...)
	if err != nil {
		return nil, nil, err
	}
	mdb := &metricDB{DB: tx, recorder: d.recorder}
	return WithDB(ctx, mdb), mdb, nil
}

func (d metricDB) ExecContext(ctx context.Context, query string, params ...any) (sql.Result, error) {
	start := time.Now()
	defer func() { d.recorder.Record(time.Now().Sub(start), query, params) }()
	return d.DB.(dbImpl).ExecContext(ctx, query, params...)
}
func (d metricDB) GetContext(ctx context.Context, dest any, query string, params ...any) error {
	start := time.Now()
	defer func() { d.recorder.Record(time.Now().Sub(start), query, params) }()
	return d.DB.(dbImpl).GetContext(ctx, dest, query, params...)
}
func (d metricDB) SelectContext(ctx context.Context, dest any, query string, params ...any) error {
	start := time.Now()
	defer func() { d.recorder.Record(time.Now().Sub(start), query, params) }()
	return d.DB.(dbImpl).SelectContext(ctx, dest, query, params...)
}
func (d metricDB) QueryxContext(ctx context.Context, query string, params ...any) (*sqlx.Rows, error) {
	start := time.Now()
	defer func() { d.recorder.Record(time.Now().Sub(start), query, params) }()
	return d.DB.(dbImpl).QueryxContext(ctx, query, params...)
}
