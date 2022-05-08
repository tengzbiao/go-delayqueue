package logger

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
)

func reset() {
	setLogger(nil)
}

func TestInitLogger(t *testing.T) {
	config := Config{
		Level: "debug",
	}
	err := InitLogger(config)
	assert.NoError(t, err)
	reset()
}

func TestGetLogger(t *testing.T) {
	// not yet init get default log
	log := GetLogger()
	config := Config{
		Level: "debug",
	}
	_ = InitLogger(config)
	// after init logger
	log2 := GetLogger()
	assert.NotEqual(t, log, log2)

	// the secend init logger
	config.Level = "info"
	_ = InitLogger(config)
	log3 := GetLogger()
	assert.NotEqual(t, log2, log3)
	reset()
}

func TestSetLogger(t *testing.T) {
	// not yet init get default log
	log := GetLogger()
	log1 := &mockLogger{}
	setLogger(log1)

	// after set logger
	log2 := GetLogger()
	assert.NotEqual(t, log, log2)
	assert.Equal(t, log1, log2)

	config := Config{
		Level: "degug",
	}
	_ = InitLogger(config)
	// after init logger
	log3 := GetLogger()
	assert.NotEqual(t, log2, log3)
	reset()
}

func TestRaceLogger(t *testing.T) {
	wg := sync.WaitGroup{}
	for i := 0; i < 100; i++ {
		wg.Add(3)
		go func() {
			defer wg.Done()
			setLogger(&mockLogger{})
		}()
		go func() {
			defer wg.Done()
			_ = GetLogger()
		}()
		go func() {
			defer wg.Done()
			config := Config{
				Level: "debug",
			}
			_ = InitLogger(config)
		}()
	}
	wg.Wait()
	reset()
}

type mockLogger struct {
}

func (m mockLogger) Info(args ...interface{}) {
	panic("implement me")
}

func (m mockLogger) Warn(args ...interface{}) {
	panic("implement me")
}

func (m mockLogger) Error(args ...interface{}) {
	panic("implement me")
}

func (m mockLogger) Debug(args ...interface{}) {
	panic("implement me")
}

func (m mockLogger) Infof(fmt string, args ...interface{}) {
	panic("implement me")
}

func (m mockLogger) Warnf(fmt string, args ...interface{}) {
	panic("implement me")
}

func (m mockLogger) Errorf(fmt string, args ...interface{}) {
	panic("implement me")
}

func (m mockLogger) Debugf(fmt string, args ...interface{}) {
	panic("implement me")
}