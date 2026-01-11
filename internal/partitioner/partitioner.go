package partitioner

import (
	"regexp"
	"strings"

	"github.com/tidwall/gjson"
)

// PathPartitioner extracts partition keys from messages based on a template
type PathPartitioner struct {
	template   string
	fieldNames []string
	parts      []templatePart
}

type templatePart struct {
	isField bool
	value   string
}

var templateRegex = regexp.MustCompile(`\{\{(\w+)\}\}`)

// NewPathPartitioner creates a new partitioner with the given template
// Template format: "prefix/{{field1}}/{{field2}}/suffix/"
func NewPathPartitioner(template string) *PathPartitioner {
	p := &PathPartitioner{
		template: template,
	}

	// Find all field placeholders
	matches := templateRegex.FindAllStringSubmatchIndex(template, -1)

	lastEnd := 0
	for _, match := range matches {
		// Add literal part before this match
		if match[0] > lastEnd {
			p.parts = append(p.parts, templatePart{
				isField: false,
				value:   template[lastEnd:match[0]],
			})
		}

		// Add field part
		fieldName := template[match[2]:match[3]]
		p.parts = append(p.parts, templatePart{
			isField: true,
			value:   fieldName,
		})
		p.fieldNames = append(p.fieldNames, fieldName)

		lastEnd = match[1]
	}

	// Add trailing literal part
	if lastEnd < len(template) {
		p.parts = append(p.parts, templatePart{
			isField: false,
			value:   template[lastEnd:],
		})
	}

	return p
}

// GetPartitionPath extracts the partition path from a JSON message
func (p *PathPartitioner) GetPartitionPath(jsonData []byte) string {
	var sb strings.Builder

	for _, part := range p.parts {
		if part.isField {
			result := gjson.GetBytes(jsonData, part.value)
			if result.Exists() {
				sb.WriteString(result.String())
			}
		} else {
			sb.WriteString(part.value)
		}
	}

	return sb.String()
}

// GetPartitionPathFromParsed extracts the partition path from pre-parsed fields
func (p *PathPartitioner) GetPartitionPathFromParsed(fields map[string]string) string {
	var sb strings.Builder

	for _, part := range p.parts {
		if part.isField {
			if val, ok := fields[part.value]; ok {
				sb.WriteString(val)
			}
		} else {
			sb.WriteString(part.value)
		}
	}

	return sb.String()
}

// GetFieldNames returns the list of field names used in the template
func (p *PathPartitioner) GetFieldNames() []string {
	return p.fieldNames
}

// ExtractFields extracts all fields needed for partitioning from JSON
func (p *PathPartitioner) ExtractFields(jsonData []byte) map[string]string {
	fields := make(map[string]string, len(p.fieldNames))
	for _, name := range p.fieldNames {
		result := gjson.GetBytes(jsonData, name)
		if result.Exists() {
			fields[name] = result.String()
		}
	}
	return fields
}

// Event represents a parsed event with partitioning info
type Event struct {
	// Raw JSON data
	Data []byte
	// Partition path for this event
	PartitionPath string
	// Kafka timestamp
	Timestamp int64
	// Extracted top-level fields for sorting
	Fields map[string]interface{}
}

// ParseEvent creates an Event from raw message data
func (p *PathPartitioner) ParseEvent(data []byte, timestamp int64) *Event {
	path := p.GetPartitionPath(data)
	return &Event{
		Data:          data,
		PartitionPath: path,
		Timestamp:     timestamp,
		Fields:        make(map[string]interface{}),
	}
}
