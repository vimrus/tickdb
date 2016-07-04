package storage

import (
	"time"
)

const TimeFormat string = "2006-01-02 15:04:05"

type Time struct {
	Time time.Time
	TS   int64
}

func NewTime(tm int64) Time {
	t := time.Unix(0, tm)
	return Time{
		Time: t,
		TS:   tm,
	}
}

func (t *Time) Level() uint16 {
	if t.Time.Nanosecond()%1e3 != 0 {
		return LevelNSecond
	}
	if t.Time.Nanosecond()%1e6 != 0 {
		return LevelUSecond
	}
	if t.Time.Nanosecond()%1e9 != 0 {
		return LevelMSecond
	}
	if t.Time.Second() != 0 {
		return LevelSecond
	}
	if t.Time.Minute() != 0 {
		return LevelMinute
	}
	if t.Time.Hour() != 0 {
		return LevelHour
	}
	if t.Time.Day() != 1 {
		return LevelDay
	}
	if t.Time.Month() != 1 {
		return LevelMonth
	}
	return LevelYear
}

func (t *Time) Timestamp(level uint16) int64 {
	var tp string
	var tm time.Time
	switch level {
	case LevelYear:
		tp = t.Time.Format("2006")
		tm, _ = time.ParseInLocation(TimeFormat, tp+"-01-01 00:00:00", time.Local)
	case LevelMonth:
		tp = t.Time.Format("2006-01")
		tm, _ = time.ParseInLocation(TimeFormat, tp+"-01 00:00:00", time.Local)
	case LevelDay:
		tp = t.Time.Format("2006-01-02 00:00:00")
		tm, _ = time.ParseInLocation(TimeFormat, tp, time.Local)
	case LevelHour:
		tp = t.Time.Format("2006-01-02 15:00:00")
		tm, _ = time.ParseInLocation(TimeFormat, tp, time.Local)
	case LevelMinute:
		tp = t.Time.Format("2006-01-02 15:04:00")
		tm, _ = time.ParseInLocation(TimeFormat, tp, time.Local)
	case LevelSecond:
		tp = t.Time.Format("2006-01-02 15:04:05")
		tm, _ = time.ParseInLocation(TimeFormat, tp, time.Local)
	case LevelMSecond:
		tm = time.Unix(0, (t.Time.UnixNano()/1e6)*1e6)
	case LevelUSecond:
		tm = time.Unix(0, (t.Time.UnixNano()/1e3)*1e3)
	default:
		tm = t.Time
	}

	return tm.UnixNano()
}
