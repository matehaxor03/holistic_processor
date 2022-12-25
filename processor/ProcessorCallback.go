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
	class "github.com/matehaxor03/holistic_db_client/class"
	json "github.com/matehaxor03/holistic_json/json"
	thread_safe "github.com/matehaxor03/holistic_thread_safe/thread_safe"
)


type ProcessorCallback struct {
	Start func()
	WakeUp func()
	SendMessageToQueueFireAndForget func(message *json.Map)
	SendMessageToQueue func(*json.Map) (*json.Map, []error)
}

func NewProcessorCallback(domain_name class.DomainName, port string) (*ProcessorCallback, []error) {	
	retry_lock := &sync.Mutex{}
	retry_condition := sync.NewCond(retry_lock)
	wakeup_lock := &sync.Mutex{}

	callback_queue := thread_safe.NewQueue()

	domain_name_value, domain_name_value_errors := domain_name.GetDomainName()
	if domain_name_value_errors != nil {
		return nil, domain_name_value_errors
	}

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

	sendMessageToQueue := func(message *json.Map) (*json.Map, []error) {
		var errors []error
		var json_payload_callback_builder strings.Builder
		callback_payload_as_string_errors := message.ToJSONString(&json_payload_callback_builder)
		if callback_payload_as_string_errors != nil {
			return nil, callback_payload_as_string_errors
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
			return nil, errors
		}	

		callback_response_json_payload, response_json_payload_errors := json.ParseJSON(string(callback_response_body_payload))
		if response_json_payload_errors != nil {
			return nil, response_json_payload_errors
		} 

		keys := callback_response_json_payload.Keys()
		if len(keys) != 1 {
			errors = append(errors, fmt.Errorf("keys length is not 1"))
			return nil, errors
		}

		response_queue := keys[0]
		json_payload_inner, json_payload_inner_errors := callback_response_json_payload.GetMap(response_queue)
		if json_payload_inner_errors != nil {
			return nil, json_payload_inner_errors
		} else if common.IsNil(json_payload_inner) {
			errors = append(errors, fmt.Errorf("payload body is nil"))
			return nil, errors
		}

		message_trace_id, message_trace_id_errors := json_payload_inner.GetString("[trace_id]")
		if message_trace_id_errors != nil {
			return nil, message_trace_id_errors
		} else if message_trace_id == nil {
			errors = append(errors, fmt.Errorf("message_trace_id is nil"))
			return nil, errors
		}

		async, async_errors := json_payload_inner.GetBool("[async]")
		if async_errors != nil {
			return nil, async_errors
		} else if common.IsNil(async) {
			errors = append(errors, fmt.Errorf("async is nil"))
			return nil, errors
		}

		callback_errors, callback_errors_errors := json_payload_inner.GetErrors("[errors]")
		if callback_errors_errors != nil {
			return nil, callback_errors_errors
		} else if common.IsNil(callback_errors) {
			errors = append(errors, fmt.Errorf("callback_errors is nil"))
			return nil, errors
		} else if len(callback_errors) > 0 {
			return nil, callback_errors
		}	
	
		return callback_response_json_payload, nil
	}

	wakeUp := func() {
		wakeup_lock.Lock()
		defer wakeup_lock.Unlock()
		(*retry_condition).Signal()
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
			fmt.Println("starting processor callback")
			go func(queue_url string) {
				fmt.Println("started processor callback")
				for {
					time.Sleep(1 * time.Nanosecond) 
					result := getAndRemoveFront()
					if common.IsNil(result) {
						retry_lock.Lock()
						(*retry_condition).Wait()
						retry_lock.Unlock()
						continue
					}

					_, response_errors := sendMessageToQueue(result)
					if response_errors != nil {
						pushFront(result)
						time.Sleep(10 * time.Second) 
						continue
					}
				}
			}(queue_url)
		},
	}
	
	heart_beat := func() {
		for range time.Tick(time.Second * 60) {
			x.WakeUp()
		}
	}
	go heart_beat()

	return &x, nil
}