package internal

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"

	"sigs.k8s.io/yaml"
)

// Guards against malformed manifests being shipped.
func TestManifestsParse(t *testing.T) {
	files, _ := filepath.Glob("../config/*/*.yaml")
	if len(files) == 0 {
		t.Fatal("no manifests found")
	}
	for _, f := range files {
		b, err := os.ReadFile(f)
		if err != nil {
			t.Fatal(err)
		}
		for i, doc := range bytes.Split(b, []byte("\n---\n")) {
			var out map[string]any
			if err := yaml.Unmarshal(doc, &out); err != nil {
				t.Errorf("%s doc %d: %v", f, i, err)
			}
		}
	}
}
