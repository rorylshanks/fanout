package partitioner

import "testing"

func TestPartitionerPaths(t *testing.T) {
	p := NewPathPartitioner("team={{team_id}}/event={{event}}/date={{date}}/")

	data := []byte(`{"team_id":123,"event":"click","date":"2024-01-01"}`)
	path := p.GetPartitionPath(data)
	if path != "team=123/event=click/date=2024-01-01/" {
		t.Fatalf("unexpected path: %q", path)
	}

	fields := p.ExtractFields(data)
	if fields["team_id"] != "123" || fields["event"] != "click" {
		t.Fatalf("unexpected fields: %+v", fields)
	}

	parsedPath := p.GetPartitionPathFromParsed(map[string]string{
		"team_id": "321",
		"event":   "signup",
		"date":    "2024-02-02",
	})
	if parsedPath != "team=321/event=signup/date=2024-02-02/" {
		t.Fatalf("unexpected parsed path: %q", parsedPath)
	}
}

func TestPartitionerParseEvent(t *testing.T) {
	p := NewPathPartitioner("team={{team_id}}/")
	event := p.ParseEvent([]byte(`{"team_id":42}`), 1700000000)
	if event.PartitionPath != "team=42/" {
		t.Fatalf("unexpected partition path: %q", event.PartitionPath)
	}
	if event.Timestamp != 1700000000 {
		t.Fatalf("unexpected timestamp: %d", event.Timestamp)
	}
	if event.Fields == nil {
		t.Fatal("expected fields map to be initialized")
	}
}

func TestPartitionerFieldNames(t *testing.T) {
	p := NewPathPartitioner("a={{a}}/b={{b}}/c={{c}}")
	names := p.GetFieldNames()
	if len(names) != 3 {
		t.Fatalf("expected 3 field names, got %d", len(names))
	}
}

func TestPartitionerMissingFields(t *testing.T) {
	p := NewPathPartitioner("team={{team_id}}/event={{event}}/date={{date}}/")

	// Missing some fields
	data := []byte(`{"team_id":123}`)
	path := p.GetPartitionPath(data)

	// Missing fields should result in empty values in the path
	if path != "team=123/event=/date=/" {
		t.Fatalf("expected path with empty values for missing fields, got: %q", path)
	}

	// All fields missing
	data = []byte(`{"other":"field"}`)
	path = p.GetPartitionPath(data)
	if path != "team=/event=/date=/" {
		t.Fatalf("expected all empty fields, got: %q", path)
	}
}

func TestPartitionerInvalidJSON(t *testing.T) {
	p := NewPathPartitioner("team={{team_id}}/event={{event}}/")

	// Invalid JSON - gjson returns empty for unparseable data
	data := []byte(`not valid json`)
	path := p.GetPartitionPath(data)

	// Should return template with empty field values
	if path != "team=/event=/" {
		t.Fatalf("expected empty fields for invalid JSON, got: %q", path)
	}

	// Empty input
	data = []byte(``)
	path = p.GetPartitionPath(data)
	if path != "team=/event=/" {
		t.Fatalf("expected empty fields for empty input, got: %q", path)
	}

	// Truncated JSON
	data = []byte(`{"team_id":123,"event":"cli`)
	path = p.GetPartitionPath(data)
	// gjson may partially parse this
	if path == "" {
		t.Fatal("expected non-empty path even for truncated JSON")
	}
}

func TestPartitionerEmptyTemplate(t *testing.T) {
	p := NewPathPartitioner("")
	data := []byte(`{"team_id":123}`)
	path := p.GetPartitionPath(data)

	if path != "" {
		t.Fatalf("expected empty path for empty template, got: %q", path)
	}

	if len(p.GetFieldNames()) != 0 {
		t.Fatal("expected no field names for empty template")
	}
}

func TestPartitionerNoPlaceholders(t *testing.T) {
	p := NewPathPartitioner("static/path/only/")
	data := []byte(`{"team_id":123}`)
	path := p.GetPartitionPath(data)

	if path != "static/path/only/" {
		t.Fatalf("expected static path, got: %q", path)
	}
}

func TestPartitionerNestedFields(t *testing.T) {
	// Note: The template regex only matches \w+ (word chars), so dots are not supported
	// in template placeholders. However, gjson can still extract nested fields if
	// you use a field name that gjson understands.

	// Template with simple field names that reference nested JSON via gjson syntax
	// won't work because the regex doesn't capture the dot:
	p := NewPathPartitioner("user={{user.id}}/name={{user.name}}/")

	// Since user.id doesn't match \w+, the template isn't parsed as a field
	// The literal "{{user.id}}" remains in the output
	data := []byte(`{"user":{"id":42,"name":"alice"}}`)
	path := p.GetPartitionPath(data)

	// The partitioner doesn't support nested field syntax in templates
	// This test documents the current behavior
	if path != "user={{user.id}}/name={{user.name}}/" {
		t.Fatalf("expected template to not parse nested fields, got: %q", path)
	}

	// Verify that top-level fields still work
	p2 := NewPathPartitioner("id={{id}}/")
	data2 := []byte(`{"id":123}`)
	path2 := p2.GetPartitionPath(data2)
	if path2 != "id=123/" {
		t.Fatalf("expected top-level field extraction, got: %q", path2)
	}
}

func TestPartitionerNullValues(t *testing.T) {
	p := NewPathPartitioner("team={{team_id}}/event={{event}}/")

	// Null value
	data := []byte(`{"team_id":null,"event":"click"}`)
	path := p.GetPartitionPath(data)

	// Null should be represented as empty or "null"
	if path != "team=null/event=click/" && path != "team=/event=click/" {
		t.Fatalf("unexpected path for null value: %q", path)
	}
}

func TestPartitionerArrayValues(t *testing.T) {
	p := NewPathPartitioner("ids={{ids}}/")

	// Array value
	data := []byte(`{"ids":[1,2,3]}`)
	path := p.GetPartitionPath(data)

	// gjson converts arrays to JSON string representation
	if path == "ids=/" {
		t.Fatal("expected array to be serialized, not empty")
	}
}

func TestPartitionerExtractFieldsMissingFields(t *testing.T) {
	p := NewPathPartitioner("team={{team_id}}/event={{event}}/date={{date}}/")

	// Only some fields present
	data := []byte(`{"team_id":123}`)
	fields := p.ExtractFields(data)

	if len(fields) != 1 {
		t.Fatalf("expected 1 field, got %d", len(fields))
	}
	if fields["team_id"] != "123" {
		t.Fatalf("expected team_id=123, got %v", fields["team_id"])
	}
	if _, ok := fields["event"]; ok {
		t.Error("expected 'event' to not be in extracted fields")
	}
}

func TestPartitionerGetPartitionPathFromParsedMissingFields(t *testing.T) {
	p := NewPathPartitioner("team={{team_id}}/event={{event}}/")

	// Partial fields
	fields := map[string]string{
		"team_id": "123",
		// event is missing
	}
	path := p.GetPartitionPathFromParsed(fields)

	if path != "team=123/event=/" {
		t.Fatalf("expected empty value for missing field, got: %q", path)
	}
}
