package resp

import "time"

type RespTime struct {
	TimesArr []time.Duration
	AvgTime  int64
	MaxTime  int64
	MinTime  int64
}

func (r *RespTime) Count() {
	r.MaxTime = r.TimesArr[0].Nanoseconds()
	r.MinTime = r.TimesArr[0].Nanoseconds()
	var sum int64
	var tNan int64
	for _, v := range r.TimesArr {
		tNan = v.Nanoseconds()
		sum += tNan
		if tNan > r.MaxTime {
			r.MaxTime = tNan
		}
		if tNan < r.MinTime {
			r.MinTime = tNan
		}
	}
	r.AvgTime = sum / int64(len(r.TimesArr))
}
