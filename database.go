package main

import (
	"errors"
	"github.com/Cistern/catena"
	"github.com/dustin/seriesly/timelib"
	"log"
	"os"
	"path/filepath"
	"strings"
)

var (
	ErrDBNotFound  = errors.New("Database not found")
	ErrDBExists    = errors.New("Database exists")
	ErrKeyNotFound = errors.New("Key not found")
)

var dbConns = make(map[string]*catena.DB)

type PostData struct {
	Time  string             `json:"time"`
	Index string             `json:"index"`
	Value map[string]float64 `json:"value"`
}

func dbcreate(path string) error {
	if _, err := os.Stat(path); err == nil {
		return ErrDBExists
	}

	db, err := catena.NewDB(path, 50000, 100000)
	dbConns[path] = db
	return err
}

func dbopen(path string) (*catena.DB, error) {
	return catena.OpenDB(path, 50000, 100000)
}

func dbconn(path string) (*catena.DB, error) {
	db := dbConns[path]
	if db == nil {
		newDB, err := dbopen(path)
		if err != nil {
			return nil, err
		}
		dbConns[path] = newDB
		return newDB, nil
	}
	return db, nil
}

func dbdelete(path string) error {
	return os.Remove(path)
}

func dbBase(path string) string {
	left := 0
	right := len(path)
	if strings.HasPrefix(path, *dbRoot) {
		left = len(*dbRoot)
		if path[left] == '/' {
			left++
		}
	}
	return path[left:right]
}

func dblist(root string) []string {
	list := []string{}
	filepath.Walk(root, func(p string, info os.FileInfo, err error) error {
		if err == nil {
			if info.IsDir() && p != root {
				list = append(list, dbBase(p))
			}
		} else {
			log.Printf("Error on %#v: %v", p, err)
		}
		return nil
	})
	return list
}

func dbstore(path string, k int64, data []PostData) error {
	db, dbErr := dbconn(path)

	if dbErr != nil {
		return dbErr
	}

	rows := []catena.Row{}
	for _, item := range data {
		source := item.Index
		t, err := timelib.ParseTime(item.Time)
		if err != nil {
			return err
		}
		ts := t.Unix()
		for metric, value := range item.Value {
			rows = append(rows, catena.Row{
				Source: source,
				Metric: metric,
				Point: catena.Point{
					Timestamp: ts,
					Value:     value,
				},
			})
		}
	}
	return db.InsertRows(rows)
}

func dbget(path string, index string, field string, ts int64) (interface{}, error) {
	db, dbErr := dbconn(path)

	if dbErr != nil {
		return nil, dbErr
	}
	i, iErr := db.NewIterator(index, field)
	defer i.Close()

	if iErr != nil {
		return nil, iErr
	}

	seekErr := i.Seek(ts)
	if seekErr != nil {
		return nil, seekErr
	}

	return i.Point().Value, nil
}

func dbquery(path string, query Query) (interface{}, error) {
	db, dbErr := dbconn(path)

	if dbErr != nil {
		return nil, dbErr
	}

	return execQuery(db, query)
}
