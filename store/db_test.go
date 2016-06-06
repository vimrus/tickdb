package store

import (
	"log"
	"testing"
	"time"
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

func Test_Update(t *testing.T) {
	path := "/tmp/t"
	db, _ := Open(path, 0600)

	k := time.Now().UnixNano()
	v := map[string]float64{
		"foo": 1.1,
		"bar": 1.2,
	}

	if err := db.Put(k, v); err != nil {
		t.Fatal(err)
	}
	_, errGet := db.Get(k)
	if errGet != nil {
		t.Fatal(errGet)
	}
	if err := db.Close(); err != nil {
		log.Fatal(err)
	}
}
