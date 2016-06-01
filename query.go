package main

import (
	"github.com/Cistern/catena"
	"github.com/dustin/seriesly/timelib"
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

func parseGroup(group string) int64 {
	var chunk int64

	for i, value := range []byte(group) {
		// number between 48~57
		if value < 48 || value > 57 {
			chunk, _ = strconv.ParseInt(string([]byte(group)[0:i]), 10, 64)
			unit := string([]byte(group)[i:])
			switch unit {
			case "minute":
				fallthrough
			case "minutes":
				chunk = chunk * 60
			case "hour":
				fallthrough
			case "hours":
				chunk = chunk * 3600
			case "day":
				fallthrough
			case "days":
				chunk = chunk * 86400
			}
			break
		}
	}
	return chunk
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
func execQuery(db *catena.DB, query Query) (interface{}, error) {
	//from
	from, fromErr := timelib.ParseTime(query.From)
	if fromErr != nil {
		return nil, fromErr
	}
	fromTS := from.Unix()

	//to
	to, toErr := timelib.ParseTime(query.To)
	if toErr != nil {
		return nil, toErr
	}
	toTS := to.Unix()

	//group
	chunk := parseGroup(query.Group)

	var data []map[string]interface{}
	//fields
	for field, opts := range query.Fields {
		var timestamps []int64
		var result []float64

		reducer := opts.Reducer
		i, iErr := db.NewIterator(query.Index, field)
		defer i.Close()

		if iErr != nil {
			return nil, iErr
		}
		seekErr := i.Seek(fromTS)
		if seekErr != nil {
			return nil, seekErr
		}
		offset := fromTS
		ts := i.Point().Timestamp

		switch reducer {
		case "first":
			for ts <= toTS {
				if ts >= offset {
					timestamps = append(timestamps, i.Point().Timestamp)
					result = append(result, i.Point().Value)
					offset = offset + chunk
				}
				seekErr = i.Next()
				if seekErr != nil {
					break
				}
				ts = i.Point().Timestamp
			}
		case "last":
			for ts <= toTS {
				if ts >= offset {
					timestamps = append(timestamps, i.Point().Timestamp)
					result = append(result, i.Point().Value)
					offset = offset + chunk
				} else {
					result[len(result)-1] = i.Point().Value
				}

				seekErr = i.Next()
				if seekErr != nil {
					break
				}

				ts = i.Point().Timestamp
			}
		case "count":
			for ts <= toTS {
				if ts >= offset {
					timestamps = append(timestamps, i.Point().Timestamp)
					result = append(result, 1)
					offset = offset + chunk
				} else {
					result[len(result)-1]++
				}

				seekErr = i.Next()
				if seekErr != nil {
					break
				}

				ts = i.Point().Timestamp
			}
		case "max":
			for ts <= toTS {
				if ts >= offset {
					timestamps = append(timestamps, i.Point().Timestamp)
					result = append(result, i.Point().Value)
					offset = offset + chunk
				} else {
					if result[len(result)-1] < i.Point().Value {
						result[len(result)-1] = i.Point().Value
					}
				}

				seekErr = i.Next()
				if seekErr != nil {
					break
				}

				ts = i.Point().Timestamp
			}
		case "min":
			for ts <= toTS {
				if ts >= offset {
					timestamps = append(timestamps, i.Point().Timestamp)
					result = append(result, i.Point().Value)
					offset = offset + chunk
				} else {
					if result[len(result)-1] > i.Point().Value {
						result[len(result)-1] = i.Point().Value
					}
				}

				seekErr = i.Next()
				if seekErr != nil {
					break
				}

				ts = i.Point().Timestamp
			}
		case "avg":
			sum := 0.0
			count := 0
			for ts <= toTS {
				if ts >= offset {
					sum = i.Point().Value
					count = 1
					timestamps = append(timestamps, i.Point().Timestamp)
					result = append(result, sum)
					offset = offset + chunk
				} else {
					count++
					sum = sum + i.Point().Value
					result[len(result)-1] = sum / float64(count)
				}

				seekErr = i.Next()
				if seekErr != nil {
					break
				}

				ts = i.Point().Timestamp
			}
		}

		if len(data) == 0 {
			for i, v := range result {
				item := make(map[string]interface{})
				item[field] = v
				ts := strconv.FormatInt(timestamps[i], 10)
				item["timestamp"] = ts
				data = append(data, item)
			}
		} else {
			for i, v := range result {
				data[i][field] = v
			}
		}
	}

	return data, nil
}
