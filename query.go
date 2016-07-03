package main

import (
	"github.com/dustin/seriesly/timelib"
	"github.com/vimrus/tickdb/storage"
	"strconv"
)

type Field struct {
	Reducer string `json:"reducer"`
}
type Query struct {
	Index  string           `json:"index"`
	From   string           `json:"from"`
	To     string           `json:"to"`
	Group  string           `json:"group"`
	Fields map[string]Field `json:"fields"`
}

func parseGroup(group string) (int, uint16) {
	var count int
	var level uint16

	for i, value := range []byte(group) {
		// number is between 48~57
		if value < 48 || value > 57 {
			count, _ = strconv.Atoi(string([]byte(group)[0:i]))
			unit := string([]byte(group)[i:])
			switch unit {
			case "second":
				fallthrough
			case "seconds":
				level = storage.LevelMinute
			case "minute":
				fallthrough
			case "minutes":
				level = storage.LevelMinute
			case "hour":
				fallthrough
			case "hours":
				level = storage.LevelHour
			case "day":
				fallthrough
			case "days":
				level = storage.LevelDay
			case "month":
				fallthrough
			case "months":
				level = storage.LevelMonth
			case "year":
				fallthrough
			case "years":
				level = storage.LevelYear
			}
			break
		}
	}
	return count, level
}

/*
	query := {
		"index": "sample",
		"from":"2016-05-31T08:00:00Z",
		"to":"2016-05-31T18:00:59Z",
		"group": "5minutes",
		"fields":{
			"open": {"reducer":"first"},
			"close": {"reducer":"last"},
		}
	}'
*/
func execQuery(db *storage.DB, query Query) (interface{}, error) {
	//from
	from, fromErr := timelib.ParseTime(query.From)
	if fromErr != nil {
		return nil, fromErr
	}
	fromTS := from.UnixNano()

	//to
	to, toErr := timelib.ParseTime(query.To)
	if toErr != nil {
		return nil, toErr
	}
	toTS := to.UnixNano()

	//group
	count, level := parseGroup(query.Group)

	reducer := make(map[string]string)

	//fields
	for field, opts := range query.Fields {
		reducer[field] = opts.Reducer
	}
	return db.Query(fromTS, toTS, level, count, reducer), nil
}
