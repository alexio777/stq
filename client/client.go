package client

import (
	"bytes"
	"errors"
	"io/ioutil"
	"net/http"
	"strconv"
	"time"
)

var (
	ErrTaskNotReady = errors.New("task not ready")
)

type Client struct {
	apiKey string
	apiURL string
}

func New(apiURL string, apikey string) *Client {
	return &Client{
		apiURL: apiURL,
		apiKey: apikey,
	}
}

func (c *Client) AddTask(queue string, timeoutSeconds int, payload []byte) error {
	req, err := http.NewRequest("POST",
		c.apiURL+"/task?queue="+queue+"&timeout="+strconv.Itoa(timeoutSeconds),
		bytes.NewBuffer(payload))
	if err != nil {
		return err
	}
	req.Header.Set("X-API-KEY", c.apiKey)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	if resp.StatusCode != http.StatusOK {
		return errors.New(resp.Status)
	}
	return nil
}

func (c *Client) WaitWorkerTask(queue string, retries int, interval time.Duration) (taskID string, payload []byte, err error) {
	req, err := http.NewRequest("GET",
		c.apiURL+"/task/worker?queue="+queue,
		nil)
	if err != nil {
		return "", nil, err
	}
	req.Header.Set("X-API-KEY", c.apiKey)
	for retry := 0; retry < retries; retry++ {
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			return "", nil, err
		}
		if resp.StatusCode != http.StatusOK {
			if resp.StatusCode == http.StatusNotFound {
				time.Sleep(interval)
				continue
			}
			return "", nil, errors.New(resp.Status)
		}
		taskIDRaw := resp.Header.Get("X-TASK-ID")
		if taskIDRaw == "" {
			return "", nil, errors.New("task id is empty")
		}
		payload, err = ioutil.ReadAll(resp.Body)
		if err != nil {
			return "", nil, err
		}
		return string(taskIDRaw), payload, nil
	}
	return "", nil, ErrTaskNotReady
}

func (c *Client) SetTaskReady(taskID string, result []byte) error {
	req, err := http.NewRequest("POST",
		c.apiURL+"/task/ready?taskid="+taskID,
		bytes.NewBuffer(result))
	if err != nil {
		return err
	}
	req.Header.Set("X-API-KEY", c.apiKey)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	if resp.StatusCode != http.StatusOK {
		return errors.New(resp.Status)
	}
	return nil
}

func (c *Client) WaitTaskReady(taskID string, retries int, interval time.Duration) ([]byte, error) {
	req, err := http.NewRequest("GET",
		c.apiURL+"/task/result?taskid="+taskID,
		nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("X-API-KEY", c.apiKey)
	for retry := 0; retry < retries; retry++ {
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			return nil, err
		}
		if resp.StatusCode != http.StatusOK {
			if resp.StatusCode == http.StatusNotFound {
				time.Sleep(interval)
				continue
			}
			return nil, errors.New(resp.Status)
		}
		result, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			return nil, err
		}
		return result, nil
	}
	return nil, ErrTaskNotReady
}
