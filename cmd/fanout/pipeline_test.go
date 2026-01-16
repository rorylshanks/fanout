package main

import (
	"testing"

	"fanout/internal/partitioner"
	"github.com/valyala/fastjson"
)

func TestGetPartitionPathFast(t *testing.T) {
	part := partitioner.NewPathPartitioner("team={{team_id}}/date={{date}}/event={{event}}/")
	p := &Pipeline{partitioner: part}

	var parser fastjson.Parser
	v, err := parser.Parse(`{"team_id":123,"event":"click","timestamp":"2024-01-02T03:04:05Z"}`)
	if err != nil {
		t.Fatalf("parse json: %v", err)
	}

	path := p.getPartitionPathFast(v)
	if path != "team=123/date=2024/01/02/event=click/" {
		t.Fatalf("unexpected partition path: %q", path)
	}
}
