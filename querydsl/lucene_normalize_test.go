package querydsl

import "testing"

func TestNormalizeLuceneQuery(t *testing.T) {
	tests := []struct {
		input    string
		expected string
	}{
		{"status:200 and host:localhost", "status:200 AND host:localhost"},
		{"error or warning", "error OR warning"},
		{"not found", "NOT found"},
		{"[1 to 10]", "[1 TO 10]"},
		{"(a and b) or c", "(a AND b) OR c"},
		{"status:200 AND host:localhost", "status:200 AND host:localhost"},
		{"standard and order", "standard AND order"},
		{"android device", "android device"},
		{"a and b and c or d not e [1 to 5]", "a AND b AND c OR d NOT e [1 TO 5]"},
		{"AND OR NOT TO", "AND OR NOT TO"},
		{"", ""},
	}

	for _, tt := range tests {
		result := NormalizeLuceneQuery(tt.input)
		if result != tt.expected {
			t.Errorf("NormalizeLuceneQuery(%q) = %q, want %q", tt.input, result, tt.expected)
		}
	}
}
