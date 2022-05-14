package main

import (
	"flag"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/tengzbiao/go-delay-queue/config"
	"github.com/tengzbiao/go-delay-queue/delay_queue"
	"github.com/tengzbiao/go-delay-queue/logger"
	"github.com/tengzbiao/go-delay-queue/routers"
)

var (
	configFile string
	queueType  string
)

const (
	QueueTypeKafka = "kafka"
	QueueTypeRedis = "redis"
)

// 解析命令行参数
func parseCommandArgs() {
	// 配置文件
	flag.StringVar(&configFile, "c", "", "./delay-queue -c /path/to/delay-queue.conf")
	// 队列类型
	flag.StringVar(&queueType, "t", "redis", "./delay-queue -t redis")
	flag.Parse()
}

func main() {
	// parse flag
	parseCommandArgs()

	// config init
	config.Init(configFile)

	// logger init
	loggerConf := logger.Config{
		Level:       config.Setting.Log.Level,
		LogFileName: config.Setting.Log.LogFileName,
		LogDir:      config.Setting.Log.LogPath,
	}
	if err := logger.InitLogger(loggerConf); err != nil {
		log.Printf("initLogger err: %v", err)
		return
	}

	var (
		done  = make(chan error, 2)
		stop  = make(chan struct{})
		queue delay_queue.Queue
		err   error
	)
	if queueType == QueueTypeRedis {
		queue, err = delay_queue.NewRedisQueue()
	} else if queueType == QueueTypeKafka {
		queue, err = delay_queue.NewKafkaQueue()
	} else {
		panic("不支持的类型")
	}
	if err != nil {
		log.Printf("init delay_queue err: %v", err)
		return
	}
	// delay队列监听
	go func() {
		done <- queue.Schedule(stop)
	}()
	// web服务
	go func() {
		done <- routers.RunWeb(queue, stop)
	}()

	// server safe close
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGHUP, syscall.SIGQUIT, syscall.SIGTERM, syscall.SIGINT)
	for {
		select {
		case err := <-done:
			logger.Errorf("Server Error Done: %v", err)
			goto END
		case s := <-sigCh:
			if s == syscall.SIGHUP {
				break
			}
			logger.Infof("Server Signal Done")
			goto END
		}
	}

END:
	close(stop)
	time.Sleep(2 * time.Second)
}
