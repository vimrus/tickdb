package store

import (
	"log"
	"testing"
)

func TestOpen(t *testing.T) {
	path := "/tmp/t"
	db, err := Open(path, 0600)
	if err != nil {
		t.Fatal(err)
	} else if db == nil {
		t.Fatal("expected db")
	}

	if s := db.Path(); s != path {
		t.Fatalf("unexpected path: %s", s)
	}

	if err := db.Close(); err != nil {
		log.Fatal(err)
	}
}
