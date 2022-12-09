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

					callback_json_bytes := []byte(json_payload_callback_builder.String())
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

					_, callback_response_body_payload_error := ioutil.ReadAll(http_callback_response.Body)
					if callback_response_body_payload_error != nil {
						pushFront(result)
						fmt.Println(callback_response_body_payload_error)
						time.Sleep(10 * time.Second) 
						continue
					}	
					
					//todo verify response
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