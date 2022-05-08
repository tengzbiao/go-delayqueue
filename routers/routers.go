package routers

import (
	"context"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"github.com/tengzbiao/go-delay-queue/config"
	"github.com/tengzbiao/go-delay-queue/delay_queue"
	"github.com/tengzbiao/go-delay-queue/logger"
)

type queueRouter struct {
	queue delay_queue.Queue
	*gin.Engine
}

func RunWeb(queue delay_queue.Queue, stop <-chan struct{}) error {
	e := gin.Default()
	r := &queueRouter{queue: queue, Engine: e}

	g := e.Group("delay-queue/api/jobs")
	{
		g.POST("add", r.Add)
		g.GET("remove", r.Remove)
		g.GET("get", r.Get)
	}

	s := http.Server{
		Addr:         config.Setting.Server.BindAddress,
		ReadTimeout:  time.Duration(config.Setting.Server.ReadTimeout) * time.Second,
		WriteTimeout: time.Duration(config.Setting.Server.WriteTimeout) * time.Second,
		Handler:      e,
	}

	go func() {
		<-stop
		logger.Infof("HTTP Server Has Been Stopped")
		_ = s.Shutdown(context.Background())
	}()

	logger.Infof("listen: %s", config.Setting.Server.BindAddress)
	return s.ListenAndServe()
}

var logicErrors = map[int]string{
	0:     "ok",
	400:   "参数错误",
	10001: "delay 取值范围1 - (2^31 - 1)",
	10002: "添加job失败",
	10003: "删除job失败",
	10004: "未查询到job",
}

func Response(ctx *gin.Context, httpCode, logicCode int, data interface{}) {
	body := new(ResponseBody)
	body.Code = logicCode
	body.Message = logicErrors[logicCode]
	body.Data = data
	ctx.JSON(httpCode, body)
}

// Add 添加job
func (r *queueRouter) Add(ctx *gin.Context) {
	var job = new(delay_queue.Job)

	if err := ctx.ShouldBindJSON(job); err != nil {
		logger.Warnf("Push BindJSON Err: %v", err)
		Response(ctx, 200, 400, nil)
		return
	}

	if job.Delay <= 0 || job.Delay > (1<<31) {
		Response(ctx, 200, 10001, nil)
		return
	}

	if job.Id == "" {
		job.Id = uuid.NewString()
	}

	if err := r.queue.AddJob(job); err != nil {
		logger.Errorf("添加job失败: %v (%s)", err, job.Id)
		Response(ctx, 200, 10002, nil)
		return
	}

	res := map[string]interface{}{
		"job_id": job.Id,
	}
	Response(ctx, 200, 0, res)
}

// Remove 完成job
func (r *queueRouter) Remove(ctx *gin.Context) {
	id := ctx.Query("job_id")

	if id == "" {
		Response(ctx, 200, 400, nil)
		return
	}

	if err := r.queue.RemoveJob(id); err != nil {
		logger.Errorf("删除job失败: %v (%s)", err, id)
		Response(ctx, 200, 10003, nil)
		return
	}

	Response(ctx, 200, 0, nil)
}

// Get 查询job
func (r *queueRouter) Get(ctx *gin.Context) {
	id := ctx.Query("job_id")

	if id == "" {
		Response(ctx, 200, 400, nil)
		return
	}

	job, err := r.queue.GetJob(id)
	if err != nil || job == nil {
		logger.Errorf("查询job失败: %v (%s)", err, id)
		Response(ctx, 200, 10004, nil)
		return
	}

	Response(ctx, 200, 0, job)
}

// ResponseBody 响应Body格式
type ResponseBody struct {
	Code    int         `json:"code"`
	Message string      `json:"message"`
	Data    interface{} `json:"data"`
}
