package querydsl

import (
	"regexp"
	"strings"
)

var luceneOperators = regexp.MustCompile(`\b(and|or|not|to)\b`)

// NormalizeLuceneQuery converts Lucene operators (and, or, not, to) to uppercase
// as required by the Lucene query parser syntax.
func NormalizeLuceneQuery(query string) string {
	return luceneOperators.ReplaceAllStringFunc(query, func(match string) string {
		return strings.ToUpper(match)
	})
}
