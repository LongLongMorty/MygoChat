package metrics

import (
	"net/http/httptest"
	"strings"
	"testing"
)

func TestPrometheusHandlerExposesMetrics(t *testing.T) {
	req := httptest.NewRequest("GET", "/prometheus", nil)
	rec := httptest.NewRecorder()
	Handler().ServeHTTP(rec, req)

	if rec.Code != 200 {
		t.Fatalf("expected 200, got %d", rec.Code)
	}
	body := rec.Body.String()
	for _, want := range []string{
		"kamachat_channel_routed_total",
		"kamachat_channel_depth",
		"kamachat_active_sessions",
		"kamachat_online_clients",
		"kamachat_cross_node_publish_total",
		"go_goroutines",
	} {
		if !strings.Contains(body, want) {
			t.Errorf("metrics output missing %q", want)
		}
	}
}
