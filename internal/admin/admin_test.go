package admin

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestServerEndpoints(t *testing.T) {
	srv := httptest.NewServer(NewServer("").Handler)
	defer srv.Close()

	tests := []struct {
		path, want string
	}{
		{"/metrics", "go_goroutines"},
		{"/debug/pprof/", "goroutine"},
		{"/debug/pprof/goroutine?debug=1", "goroutine profile"},
	}
	for _, tt := range tests {
		resp, err := http.Get(srv.URL + tt.path)
		if err != nil {
			t.Fatalf("GET %s: %v", tt.path, err)
		}
		body, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		if resp.StatusCode != http.StatusOK || !strings.Contains(string(body), tt.want) {
			t.Errorf("GET %s: status %d, body lacks %q", tt.path, resp.StatusCode, tt.want)
		}
	}

}
