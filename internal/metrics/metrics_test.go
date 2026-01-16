package metrics

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

func TestHandlerEmitsNamespaceMetrics(t *testing.T) {
	namespace := "fanout_test_metrics_" + strings.ToLower(t.Name())
	m := New(namespace)
	m.MessagesReceived.Inc()

	req := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	rec := httptest.NewRecorder()
	Handler().ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected status 200, got %d", rec.Code)
	}

	if !strings.Contains(rec.Body.String(), namespace+"_messages_received_total") {
		t.Fatalf("expected metrics output to include namespace %q", namespace)
	}
}

func TestObserveDuration(t *testing.T) {
	reg := prometheus.NewRegistry()
	hist := prometheus.NewHistogram(prometheus.HistogramOpts{
		Name: "test_duration_seconds",
		Help: "test duration",
	})
	if err := reg.Register(hist); err != nil {
		t.Fatalf("register histogram: %v", err)
	}

	start := time.Now().Add(-15 * time.Millisecond)
	ObserveDuration(hist, start, time.Millisecond)

	families, err := reg.Gather()
	if err != nil {
		t.Fatalf("gather metrics: %v", err)
	}

	var found bool
	for _, fam := range families {
		if fam.GetName() != "test_duration_seconds" {
			continue
		}
		if len(fam.Metric) == 0 || fam.Metric[0].Histogram.GetSampleCount() == 0 {
			t.Fatalf("expected histogram to record samples")
		}
		found = true
	}
	if !found {
		t.Fatalf("expected histogram metric to be gathered")
	}
}
