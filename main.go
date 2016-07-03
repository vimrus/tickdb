package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net"
	"net/http"
	"regexp"
	"time"
)

var dbRoot = flag.String("root", "db", "Root directory of database files.")

type routeHandler func(parts []string, w http.ResponseWriter, req *http.Request)

type router struct {
	Method  string
	Path    string
	Handler routeHandler
}

var routing = []router{
	router{"GET", "^/$", serverInfo},

	router{"GET", "^/_all_dbs$", listDatabases},
	router{"GET", "^/([-%+()$_a-zA-Z-1-9]+)/?$", dbInfo},
	router{"PUT", "^/([-%+()$_a-zA-Z0-9]+)/?$", createDB},
	router{"DELETE", "^/([-%+()$_a-zA-Z0-9]+)/_all$", deleteDB},

	router{"POST", "^/([-%+()$_a-zA-Z0-9]+)/_query$", query},
	router{"POST", "^/([-%+()$_a-zA-Z0-9]+)/?$", putDocuments},
	router{"GET", "^/([-%+()$_a-zA-Z0-9]+)/([^/]+)/([^/]+)$", getDocument},
	router{"DELETE", "^/([-%+()$_a-zA-Z0-9]+)/([^/]+)/_all$", removeIndex},
	router{"DELETE", "^/([-%+()$_a-zA-Z0-9]+)/([^/]+)$", removeDocuments},
}

func render(status int, w http.ResponseWriter, ob interface{}) {
	b, err := json.Marshal(ob)
	if err != nil {
		log.Fatalf("Error encoding %v.", ob)
	}
	w.Header().Set("Content-Length", fmt.Sprintf("%d", len(b)))
	w.WriteHeader(status)
	w.Write(b)
}

func emitError(status int, w http.ResponseWriter, e, reason string) {
	m := map[string]string{"error": e, "reason": reason}
	render(status, w, m)
}
func defaultHandler(parts []string, w http.ResponseWriter, req *http.Request) {
	emitError(400, w, "no_handler", fmt.Sprintf("Can't handle %v to %v\n", req.Method, req.URL.Path))
}

func findHandler(method, path string) (router, []string) {
	for _, r := range routing {
		if r.Method == method {
			matches := regexp.MustCompile(r.Path).FindAllStringSubmatch(path, 1)
			if len(matches) > 0 {
				return r, matches[0][1:]
			}
		}
	}
	return router{"DEFAULT", path, defaultHandler}, []string{}
}

func handler(w http.ResponseWriter, req *http.Request) {
	start := time.Now()

	route, hparts := findHandler(req.Method, req.URL.Path)

	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header().Set("Content-type", "application/json")
	route.Handler(hparts, w, req)

	end := time.Now()
	latency := end.Sub(start)
	log.Printf("%13v %s", latency, req.Method)
}

func main() {
	addr := flag.String("addr", ":9527", "Address to listen on")
	s := &http.Server{
		Addr:        *addr,
		Handler:     http.HandlerFunc(handler),
		ReadTimeout: 5 * time.Second,
	}

	ln, err := net.Listen("tcp", *addr)
	if err != nil {
		log.Fatalf("Error setting up listener: %v", err)
	}
	log.Printf("Listening on %s", *addr)

	s.Serve(ln)
}
