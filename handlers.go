package main

import (
	"encoding/json"
	"github.com/dustin/seriesly/timelib"
	"io/ioutil"
	"net/http"
	"path/filepath"
)

func dbPath(filename string) string {
	return filepath.Join(*dbRoot, filename)
}

func serverInfo(parts []string, w http.ResponseWriter, req *http.Request) {
	info := map[string]string{
		"tickdb":  "Welcome",
		"version": "0.0",
	}
	render(200, w, info)
}

func createDB(args []string, w http.ResponseWriter, req *http.Request) {
	path := dbPath(args[0])
	err := dbcreate(path)

	if err == nil {
		w.WriteHeader(201)
	} else {
		emitError(500, w, "Server Error", err.Error())
	}
}

func dbInfo(args []string, w http.ResponseWriter, req *http.Request) {
	path := dbPath(args[0])
	_, err := dbconn(path)
	if err != nil {
		emitError(500, w, "Error opening DB", err.Error())
		return
	}

	render(200, w, map[string]interface{}{
		"name": args[0],
	})
}

func deleteDB(args []string, w http.ResponseWriter, req *http.Request) {
	path := dbPath(args[0])
	err := dbdelete(path)
	if err == nil {
		w.WriteHeader(201)
	} else {
		emitError(500, w, "Server Error", err.Error())
	}
}

func listDatabases(args []string, w http.ResponseWriter, req *http.Request) {
	render(200, w, dblist(*dbRoot))
}

func putDocuments(args []string, w http.ResponseWriter, req *http.Request) {
	defer req.Body.Close()

	var ts int64
	path := dbPath(args[0])

	result, bodyErr := ioutil.ReadAll(req.Body)
	if bodyErr != nil {
		emitError(500, w, "Server Error", bodyErr.Error())
	}

	var data []PostData
	json.Unmarshal(result, &data)
	err := dbstore(path, ts, data)
	if err != nil {
		emitError(500, w, "Server Error", err.Error())
	}

	render(200, w, "success")
}

func query(args []string, w http.ResponseWriter, req *http.Request) {
	path := dbPath(args[0])

	result, bodyErr := ioutil.ReadAll(req.Body)
	if bodyErr != nil {
		emitError(500, w, "Server Error", bodyErr.Error())
	}

	var query Query
	json.Unmarshal(result, &query)

	data, err := dbquery(path, query)
	if err != nil {
		emitError(500, w, "Server Error", err.Error())
	} else {
		render(200, w, data)
	}
}

func getDocument(args []string, w http.ResponseWriter, req *http.Request) {
	path := dbPath(args[0])
	index := args[1]
	field := args[2]
	t, err := timelib.ParseTime(args[3])
	if err != nil {
		emitError(400, w, "Bad time format", err.Error())
	}
	ts := t.Unix()
	doc, err := dbget(path, index, field, ts)
	if err != nil {
		emitError(500, w, "Server Error", err.Error())
	} else {
		render(200, w, doc)
	}
}

func removeDocuments(args []string, w http.ResponseWriter, req *http.Request) {
}
