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

	render(200, w, map[string]interface{}{
		"name": args[0],
		"path": path,
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
	} else {
		var data []PostData
		json.Unmarshal(result, &data)
		err := dbstore(path, ts, data)
		if err != nil {
			emitError(500, w, "Server Error", err.Error())
		} else {
			render(200, w, "success")
		}
	}

}

func query(args []string, w http.ResponseWriter, req *http.Request) {
	path := dbPath(args[0])

	result, bodyErr := ioutil.ReadAll(req.Body)
	if bodyErr != nil {
		emitError(500, w, "Server Error", bodyErr.Error())
	} else {
		var query Query
		json.Unmarshal(result, &query)

		data, err := dbquery(path, query)
		if err != nil {
			emitError(500, w, "Server Error", err.Error())
		} else {
			render(200, w, data)
		}
	}
}

func getDocument(args []string, w http.ResponseWriter, req *http.Request) {
	path := dbPath(args[0])
	index := args[1]
	t, err := timelib.ParseTime(args[2])
	if err != nil {
		emitError(400, w, "Bad time format", err.Error())
	} else {
		ts := t.UnixNano()
		doc, err := dbget(path, index, ts)
		if err != nil {
			emitError(500, w, "Server Error", err.Error())
		} else {
			render(200, w, doc)
		}
	}
}

func removeIndex(args []string, w http.ResponseWriter, req *http.Request) {
	path := dbPath(args[0])
	index := args[1]
	err := indexdelete(path, index)
	if err == nil {
		w.WriteHeader(201)
	} else {
		emitError(500, w, "Server Error", err.Error())
	}
}

func removeDocuments(args []string, w http.ResponseWriter, req *http.Request) {
	path := dbPath(args[0])
	index := args[1]

	result, bodyErr := ioutil.ReadAll(req.Body)
	if bodyErr != nil {
		emitError(500, w, "Server Error", bodyErr.Error())
		return
	}
	if len(result) == 0 {
		w.WriteHeader(201)
		return
	}

	var query map[string]string
	json.Unmarshal(result, &query)

	if query["from"] != "" && query["to"] != "" {
		fromTime, err := timelib.ParseTime(query["from"])
		if err != nil {
			emitError(500, w, "Time 'from' Error", err.Error())
			return
		}
		from := fromTime.UnixNano()

		toTime, err := timelib.ParseTime(query["to"])
		if err != nil {
			emitError(500, w, "Time 'to' Error", err.Error())
		} else {
			to := toTime.UnixNano()
			err := pointremove(path, index, from, to)
			if err == nil {
				w.WriteHeader(201)
			} else {
				emitError(500, w, "Server Error", err.Error())
			}
		}
	} else {
		emitError(500, w, "Time 'to' Error", "'from' and 'to' time required")
	}
}
