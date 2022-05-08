# go-delay-queue

1. 支持基于Redis实现的延迟队列
2. 支持基于Kafka实现的延迟队列

## 应用场景
* 预定会议后，需要在预定的时间点前15分钟通知与会人员参加会议
* 用户发起退款，如果三天内没有得到处理则通知相关运营人员
* 用户注册成功后，如果三天内没有登陆则进行短信提醒
* 订单超过30分钟未支付，自动关闭
* 订单完成后, 如果用户一直未评价, 5天后自动好评
* 会员到期前15天, 到期前3天分别发送短信提醒
* ...

## Redis延迟队列实现原理
- 利用Redis的有序集合，member为jobId, score为任务执行的时间戳 
- 每秒扫描一次集合，取出执行时间小于等于当前时间的任务.

## Kafka延迟队列实现原理
- 利用Kafka的assign为consumer手动、显示的指定需要消费的topic-partitions，使用提供的暂停、恢复api和重置offset实现延迟消费。
- 启用多个消费实例时，会对配置中指定的PartitionGroups实行抢占订阅，进程退出时退订。
- Kafka需要事先确定好延迟时间的topic

## Job存储
> redis和kafka的延迟队列只流转jobId，具体任务存储在redis，并使用msgpack高效压缩消息体。

## 依赖
* [Redis](github.com/garyburd/redigo/redis)
* [Kafka](https://github.com/confluentinc/confluent-kafka-go)
* [Gin](github.com/gin-gonic/gin)

## 安装
* `go get -d github.com/tengzbiao/go-delay-queue`
* `go build -o delay-queue cmd/main.go`

## 运行
Redis队列
`./delay-queue -c config/delay-queue.conf -t redis`

Kafka队列
`./delay-queue -c config/delay-queue.conf -t kafka`

HTTP Server监听
`0.0.0.0:9277`

## 消息消费
> 任务到期投递到业务消费队列，消费参照examples

## HTTP接口

* 请求方法 `POST`
* 请求Body及返回值均为`json`

|  参数名 |     类型    |     含义     |        备注       |
|:-------:|:-----------:|:------------:|:-----------------:|
|   code  |     int     |    状态码    | 0: 成功 非0: 失败 |
| message |    string   | 状态描述信息 |                   |
|   data  | object, null |   附加信息   |                   |

### 添加任务
URL地址 `/delay-queue/api/jobs/add`
```json
{
  "topic": "order",
  "delay": 3600,
  "body": "{\"uid\": 10829378,\"created\": 1498657365 }"
}
```
|  参数名 |     类型    |     含义     |        备注       |
|:-------:|:-----------:|:------------:|:-----------------:|
|   id  | string     |    Job唯一id(不传则系统会自动生成一个，传递时务必保证唯一)                  |
|   topic  | string     |    Job类型, 消息到期真正投递的队列名                 |                     |
|   delay  | int        |    Job需要延迟的时间, 单位：秒    |                   |
|   body   | string     |    自定义job内容 |                   |


### 移除任务
URL地址 `/delay-queue/api/jobs/remove`

```json
{
  "job_id": "15702398321"
}
```

|  参数名 |     类型    |     含义     |        备注       |
|:-------:|:-----------:|:------------:|:-----------------:|
|   job_id  | string     |    Job唯一标识    |                     |


### 查询任务
URL地址 `/delay-queue/api/jobs/get`

```json
{
  "job_id": "15702398321"
}
```

|  参数名 |     类型    |     含义     |        备注       |
|:-------:|:-----------:|:------------:|:-----------------:|
|   job_id  | string     |    Job唯一标识       |            |


返回值
```json
{
    "code": 0,
    "message": "操作成功",
    "data": {
        "topic": "order",
        "id": "15702398321",
        "exec_time": 1506787453,
        "delay": 10,
        "body": "{\"uid\": 10829378,\"created\": 1498657365 }"
    
    }
}
```

|  参数名 |     类型    |     含义     |        备注       |
|:-------:|:-----------:|:------------:|:-----------------:|
|   topic  | string     |    Job类型, 消息到期真正投递的队列名               |                     |
|   id     | string     |    Job唯一标识           |                   |
|   delay  | int        |    Job延迟时间，单位：秒    |                   |
|   exec_time | int     |    Job延迟执行的时间戳    |                   |
|   body   | string     |    Job内容，供消费者做具体的业务处理 |


Job不存在返回值
```json
{
  "code": 0,
  "message": "操作成功",
  "data": null
}
```
  