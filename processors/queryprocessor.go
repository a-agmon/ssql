package processors

import (
	"fmt"
	"regexp"
)

// Query declare the query struct that contains all parts of query
// which are entity, filter and select components
type Query struct {
	Entity string
	Filter string
	Select string
}

type QueryProcessor struct {
	re *regexp.Regexp
}

func NewQueryProcessor() *QueryProcessor {
	r := regexp.MustCompile(`(?i)^([\w\.\-]+)\[select:((?:\w+,?\s*)+)]\[filter:((?:[\w="\.\-]+,?\s*)+)]$`)
	return &QueryProcessor{re: r}
}

// Process validate and extract the query parts from the payload
func (p *QueryProcessor) Process(payload string) (*Query, error) {
	match := p.re.FindStringSubmatch(payload)
	if match == nil || len(match) < 4 {
		return nil, fmt.Errorf("invalid payload")
	}
	entityPart := match[1]
	filterPart := match[2]
	selectPart := match[3]
	return &Query{
		Entity: entityPart,
		Filter: filterPart,
		Select: selectPart,
	}, nil
}
