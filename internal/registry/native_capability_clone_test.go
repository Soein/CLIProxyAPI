package registry

import "testing"

func TestToolSearchCapabilityCloneIsolation(t *testing.T) {
	for _, mutation := range []string{"registration input", "returned model"} {
		t.Run(mutation, func(t *testing.T) {
			registry := newTestModelRegistry()
			toolSearch := true
			input := &ModelInfo{ID: "tool-search-clone", NativeCapabilities: &NativeCapabilities{ToolSearch: &toolSearch}}
			registry.RegisterClient("clone-client", "claude", []*ModelInfo{input})
			if mutation == "registration input" {
				*input.NativeCapabilities.ToolSearch = false
			} else {
				models := registry.GetModelsForClient("clone-client")
				if len(models) != 1 || models[0].NativeCapabilities == nil || models[0].NativeCapabilities.ToolSearch == nil {
					t.Fatal("registered tool-search capability missing")
				}
				*models[0].NativeCapabilities.ToolSearch = false
			}
			models := registry.GetModelsForClient("clone-client")
			if len(models) != 1 || models[0].NativeCapabilities == nil || models[0].NativeCapabilities.ToolSearch == nil || !*models[0].NativeCapabilities.ToolSearch {
				t.Fatalf("%s mutation changed registered tool-search capability", mutation)
			}
		})
	}
}
