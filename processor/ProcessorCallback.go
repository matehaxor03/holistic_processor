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
	PushBack func(message *json.Map)
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

	wakeUp := func() {
		wakeup_lock.Lock()
		defer wakeup_lock.Unlock()
		(*retry_condition).Signal()
	}

	x := ProcessorCallback{
		WakeUp: func() {
			wakeUp()
		},
		PushBack: func(message *json.Map) {
			pushBack(message)
			wakeUp()
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

					var json_payload_callback_builder strings.Builder
					callback_payload_as_string_errors := result.ToJSONString(&json_payload_callback_builder)
					if callback_payload_as_string_errors != nil {
						pushFront(result)
						fmt.Println(callback_payload_as_string_errors)
						time.Sleep(10 * time.Second) 
						continue
					}

					callback_json_bytes_string := json_payload_callback_builder.String()
					fmt.Println(callback_json_bytes_string)
					
					callback_json_bytes := []byte(callback_json_bytes_string)
					callback_json_reader := bytes.NewReader(callback_json_bytes)
					callback_request, callback_request_error := http.NewRequest(http.MethodPost, queue_url, callback_json_reader)

					if callback_request_error != nil {
						pushFront(result)
						fmt.Println(callback_request_error)
						time.Sleep(10 * time.Second) 
						continue
					}

					http_callback_response, http_callback_response_error := http_client.Do(callback_request)
					if http_callback_response_error != nil {
						pushFront(result)
						fmt.Println(http_callback_response_error)
						time.Sleep(10 * time.Second) 
						continue
					}

					callback_response_body_payload, callback_response_body_payload_error := ioutil.ReadAll(http_callback_response.Body)
					if callback_response_body_payload_error != nil {
						pushFront(result)
						fmt.Println(callback_response_body_payload_error)
						time.Sleep(10 * time.Second) 
						continue
					}	

					callback_response_json_payload, response_json_payload_errors := json.ParseJSON(string(callback_response_body_payload))
					if response_json_payload_errors != nil {
						pushFront(result)
						fmt.Println(response_json_payload_errors)
						fmt.Println(string(callback_response_body_payload))
						time.Sleep(10 * time.Second) 
						continue
					} 

					keys := callback_response_json_payload.Keys()
					if len(keys) != 1 {
						pushFront(result)
						fmt.Println("keys length is not 1")
						fmt.Println(string(callback_response_body_payload))
						continue
					}

					response_queue := keys[0]
					json_payload_inner, json_payload_inner_errors := callback_response_json_payload.GetMap(response_queue)
					if json_payload_inner_errors != nil {
						pushFront(result)
						fmt.Println(json_payload_inner_errors) 
						fmt.Println(string(callback_response_body_payload))
						continue
					} else if common.IsNil(json_payload_inner) {
						pushFront(result)
						fmt.Println("payload body is nil") 
						fmt.Println(string(callback_response_body_payload))
						continue
					}

					message_trace_id, message_trace_id_errors := json_payload_inner.GetString("[trace_id]")
					if message_trace_id_errors != nil {
						pushFront(result)
						fmt.Println(message_trace_id_errors) 
						fmt.Println(string(callback_response_body_payload))
						continue
					} else if message_trace_id == nil {
						pushFront(result)
						fmt.Println("message_trace_id is nil")
						fmt.Println(string(callback_response_body_payload))
						continue
					}

					async, async_errors := json_payload_inner.GetBool("[async]")
					if async_errors != nil {
						pushFront(result)
						fmt.Println(message_trace_id_errors) 
						fmt.Println(string(callback_response_body_payload))
						continue
					} else if common.IsNil(async) {
						pushFront(result)
						fmt.Println("async is nil") 
						fmt.Println(string(callback_response_body_payload))
						continue
					}

					callback_errors, callback_errors_errors := json_payload_inner.GetErrors("[errors]")
					if callback_errors_errors != nil {
						pushFront(result)
						fmt.Println(callback_errors_errors) 
						fmt.Println(string(callback_response_body_payload))
						continue
					} else if common.IsNil(callback_errors) {
						pushFront(result)
						fmt.Println("callback_errors is nil") 
						fmt.Println(string(callback_response_body_payload))
						continue
					} else if len(callback_errors) > 0 {
						pushFront(result)
						fmt.Println("callback_errors is greater than 0") 
						fmt.Println(string(callback_response_body_payload))
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