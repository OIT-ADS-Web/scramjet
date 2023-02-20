package scramjet

import (
    "time"
)

func TimestampString() string {
	t := time.Now().UTC()
    return t.Format("20060102150405")
}