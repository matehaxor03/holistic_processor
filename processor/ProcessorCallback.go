package processor

import (
	"fmt"
	"io/ioutil"
	"net/http"
	"bytes"
	"crypto/tls"
	"time"
	"sync"
	"strings"
	common "github.com/matehaxor03/holistic_common/common"
	dao "github.com/matehaxor03/holistic_db_client/dao"
	json "github.com/matehaxor03/holistic_json/json"
	thread_safe "github.com/matehaxor03/holistic_thread_safe/thread_safe"
)


type ProcessorCallback struct {
	Start func()
	WakeUp func()
	SendMessageToQueueFireAndForget func(message *json.Map)
	SendMessageToQueue func(*json.Map) (*json.Map, []error)
}

func NewProcessorCallback(domain_name dao.DomainName, port string) (*ProcessorCallback, []error) {	
	status := "not started"
	status_lock := &sync.Mutex{}
	var wg sync.WaitGroup
	wakeup_lock := &sync.Mutex{}

	callback_queue := thread_safe.NewQueue()

	domain_name_value := domain_name.GetDomainName()

	queue_url := fmt.Sprintf("https://%s:%s/", domain_name_value, port)
	transport_config := &http.Transport{
		TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
	}

	http_client := http.Client{
		Timeout: 120 * time.Second,
		Transport: transport_config,
	}

	pushBack := func(message *json.Map) {
		callback_queue.PushBack(message)
	}

	pushFront := func(message *json.Map) {
		callback_queue.PushFront(message)
	}

	getAndRemoveFront := func() *json.Map {
		return callback_queue.GetAndRemoveFront()
	}

	getLen := func() uint64 {
		return callback_queue.Len()
	}

	get_or_set_status := func(s string) string {
		status_lock.Lock()
		defer status_lock.Unlock()
		if s == "" {
			return status
		} else {
			status = s
			return ""
		}
	}

	sendMessageToQueue := func(message *json.Map) (*json.Map, []error) {
		var errors []error
		var json_payload_callback_builder strings.Builder
		callback_payload_as_string_errors := message.ToJSONString(&json_payload_callback_builder)
		if callback_payload_as_string_errors != nil {
			errors = append(errors, callback_payload_as_string_errors...)
			return nil, errors
		}

		callback_json_bytes_string := json_payload_callback_builder.String()
		callback_json_bytes := []byte(callback_json_bytes_string)
		callback_json_reader := bytes.NewReader(callback_json_bytes)
		callback_request, callback_request_error := http.NewRequest(http.MethodPost, queue_url, callback_json_reader)

		if callback_request_error != nil {
			errors = append(errors, callback_request_error)
			return nil, errors
		}

		http_callback_response, http_callback_response_error := http_client.Do(callback_request)
		if http_callback_response_error != nil {
			errors = append(errors, http_callback_response_error)
			return nil, errors
		}

		callback_response_body_payload, callback_response_body_payload_error := ioutil.ReadAll(http_callback_response.Body)
		if callback_response_body_payload_error != nil {
			errors = append(errors, callback_response_body_payload_error)
		} else if common.IsNil(callback_response_body_payload) {
			errors = append(errors,  fmt.Errorf("callback_response_body_payload is nil from callback"))
		}

		if len(errors) > 0 {
			return nil, errors
		}	

		callback_response_json_payload, response_json_payload_errors := json.Parse(string(callback_response_body_payload))
		if response_json_payload_errors != nil {
			errors = append(errors, response_json_payload_errors...)
		} else if common.IsNil(callback_response_json_payload) {
			errors = append(errors,  fmt.Errorf("callback_response_json_payload is nil from callback"))
		}

		if len(errors) > 0 {
			return nil, errors
		}	

		message_trace_id, message_trace_id_errors := callback_response_json_payload.GetString("[trace_id]")
		if message_trace_id_errors != nil {
			errors = append(errors, message_trace_id_errors...)
		} else if common.IsNil(message_trace_id) {
			errors = append(errors, fmt.Errorf("message_trace_id is nil from callback"))
		}

		async, async_errors := callback_response_json_payload.GetBool("[async]")
		if async_errors != nil {
			errors = append(errors, async_errors...)
		} else if common.IsNil(async) {
			errors = append(errors, fmt.Errorf("async is nil"))
		}

		callback_errors, callback_errors_errors := callback_response_json_payload.GetErrors("[errors]")
		if callback_errors_errors != nil {
			errors = append(errors, callback_errors_errors...)
		} 
		
		if !common.IsNil(callback_errors) {
			errors = append(errors, callback_errors...)
		} 
		
		if len(errors) > 0 {
			return nil, errors
		}	
	
		return callback_response_json_payload, nil
	}

	wakeUp := func() {
		wakeup_lock.Lock()
		defer wakeup_lock.Unlock()
		if get_or_set_status("") == "paused" {
			get_or_set_status("try again") 
			wg.Done()
		} else {
			get_or_set_status("try again")
		}
	}

	x := ProcessorCallback{
		WakeUp: func() {
			wakeUp()
		},
		SendMessageToQueueFireAndForget: func(message *json.Map) {
			pushBack(message)
			wakeUp()
		},
		SendMessageToQueue: func(message *json.Map) (*json.Map, []error) {
			return sendMessageToQueue(message)
		},
		Start: func() {
			go func(queue_url string) {
				for {
					get_or_set_status("running")
					time.Sleep(1 * time.Nanosecond) 
					result := getAndRemoveFront()
					if common.IsNil(result) {
						if get_or_set_status("") == "running" && getLen() == 0 {
							get_or_set_status("paused")
							wg.Add(1)
							wg.Wait()
						}
						continue
					}

					_, response_errors := sendMessageToQueue(result)
					if response_errors != nil {
						if len(response_errors) == 1 && strings.Contains(fmt.Sprintf("%s", response_errors[0]), "Duplicate entry") {
							fmt.Println("Duplicate entry found")
							fmt.Println(response_errors)
						} else {
							fmt.Println("no retry error detected")
							fmt.Println(fmt.Sprintf("%s", response_errors))
							pushFront(result)
							time.Sleep(10 * time.Second) 
						}		
						continue
					}
				}
			}(queue_url)
		},
	}
	/*
	heart_beat := func() {
		for range time.Tick(time.Second * 60) {
			x.WakeUp()
		}
	}
	go heart_beat()*/

	return &x, nil
}