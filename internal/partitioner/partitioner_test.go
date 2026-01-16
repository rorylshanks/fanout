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
