package main

import (
	"errors"
	"github.com/dustin/seriesly/timelib"
	"github.com/vimrus/tickdb/storage"
	"log"
	"os"
	"path/filepath"
	"strings"
)

var (
	ErrDBNotFound  = errors.New("Database not found")
	ErrDBExists    = errors.New("Database exists")
	ErrDBCreate    = errors.New("Create database failed")
	ErrKeyNotFound = errors.New("Key not found")
)

type indexConns map[string]*storage.DB

var dbConns = make(map[string]indexConns)

type PostData struct {
	Time  string             `json:"time"`
	Index string             `json:"index"`
	Value map[string]float64 `json:"value"`
}

func dbcreate(path string) error {
	if _, err := os.Stat(path); err == nil {
		return ErrDBExists
	}

	if err := os.Mkdir(path, 0666); err != nil {
		return ErrDBCreate
	}

	return nil
}

func dbopen(path string) error {
	if _, err := os.Stat(path); err != nil {
		return ErrDBNotFound
	}
	return nil
}

func dbconn(path, index string) (*storage.DB, error) {
	if _, ok := dbConns[path]; !ok {
		if err := dbopen(path); err != nil {
			return nil, err
		}

		idx, err := storage.Open(path + "/" + index)
		dbConns[path] = make(map[string]*storage.DB)
		dbConns[path][index] = idx
		return idx, err
	}

	if idx, ok := dbConns[path][index]; !ok {
		var err error
		idx, err = storage.Open(path + "/" + index)
		dbConns[path][index] = idx
		return idx, err
	}

	return dbConns[path][index], nil
}

func dbstore(path string, k int64, data []PostData) error {
	for _, row := range data {
		storage, dbErr := dbconn(path, row.Index)

		if dbErr != nil {
			return dbErr
		}

		t, err := timelib.ParseTime(row.Time)
		if err != nil {
			return err
		}

		err = storage.Put(t.UnixNano(), row.Value)
		if err != nil {
			return nil
		}
	}
	return nil
}

func dbget(path string, index string, ts int64) (interface{}, error) {
	db, dbErr := dbconn(path, index)
	if dbErr != nil {
		return nil, dbErr
	}

	point, err := db.Get(ts)
	if err != nil {
		return nil, err
	}
	return point.Value, nil
}

func dbquery(path string, query Query) (interface{}, error) {
	db, dbErr := dbconn(path, query.Index)

	if dbErr != nil {
		return nil, dbErr
	}

	return execQuery(db, query)
}

func dbdelete(path string) error {
	return os.Remove(path)
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

func indexdelete(path, index string) error {
	return os.Remove(path + "/" + index)
}

func pointremove(path, index string, from, to int64) error {
	storage, dbErr := dbconn(path, index)
	if dbErr != nil {
		return dbErr
	}
	storage.Delete(from, to)
	return nil
}
