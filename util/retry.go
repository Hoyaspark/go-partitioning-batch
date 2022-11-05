package util

import (
	"fmt"
	"log"
	"time"
)

func RetryFunc(attempts int, dur time.Duration, f func() error) (err error) {
	for i := 0; ; i++ {
		err = f()
		if err == nil {
			return
		}

		if i >= (attempts - 1) {
			break
		}

		<-time.After(dur)

		log.Println("retrying after error:", err)
	}
	return fmt.Errorf("after %d attempts, last error: %s", attempts, err)
}

func RetryIfFailed(attempts int, dur time.Duration, f func() (interface{}, error)) (interface{}, error) {
	var value interface{}
	var err error
	for i := 0; i <= attempts; i++ {
		value, err = f()
		if err != nil {
			<-time.After(dur)
			continue
		}
		if value != nil {
			break
		}
	}
	if err != nil {
		return nil, err
	}
	return value, nil
}
