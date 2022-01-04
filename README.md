# stq
Simple Tasks Queue with HTTP API

Environments list:

|Variable|Value|
|---|---|
|BACKEND|memory|
|LISTEN|listen address, example: localhost:11111|
|APIKEY|apikey to protect|

API:

- POST /task?queue=QUEUENAME&timeout=SECONDS and payload in body

    return task id

- GET /task/worker?queue=QUEUENAME

    return X-TASK-ID in header and payload in body

- POST /task/ready?taskid=TASKID and result in body

    set task result and return 200

- GET /task/result?taskid=TASKID

    return task result or 408 HTTP StatusRequestTimeout

- GET /stats

    return stats in json

Backends:
- memory

Docker images:

https://hub.docker.com/r/alexstup/stq/tags

Go client:

`go get -u github.com/alexio777/stq/client`