package awsstream

import "testing"

func TestHeaderValueTypeName(t *testing.T) {
	cases := []struct {
		v    HeaderValueType
		name string
	}{
		{HeaderValueTypeString, "string"},
		{HeaderValueTypeBoolTrue, "bool_true"},
		{HeaderValueTypeBoolFalse, "bool_false"},
		{HeaderValueType(99), "unknown"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := tc.v.String(); got != tc.name {
				t.Errorf("String() = %q; want %q", got, tc.name)
			}
		})
	}
}

func TestFrameStringHeader(t *testing.T) {
	f := &Frame{
		Headers: []Header{
			{Name: ":content-type", Type: HeaderValueTypeString, Value: []byte("application/json")},
			{Name: ":event-type", Type: HeaderValueTypeString, Value: []byte("contentBlock")},
		},
		Payload: []byte(`{"text":"hi"}`),
	}
	if got, ok := f.StringHeader(":event-type"); !ok || got != "contentBlock" {
		t.Errorf("StringHeader event-type = (%q, %v); want (contentBlock, true)", got, ok)
	}
	if _, ok := f.StringHeader(":missing"); ok {
		t.Errorf("StringHeader on missing key should be ok=false")
	}
}
