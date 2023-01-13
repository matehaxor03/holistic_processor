package processor

import (
	"fmt"
	//"io/ioutil"
	//"net/http"
	//"bytes"
	//"crypto/tls"
	"time"
	"sync"
	//"strings"
	//"crypto/rand"
	common "github.com/matehaxor03/holistic_common/common"
	dao "github.com/matehaxor03/holistic_db_client/dao"
	//json "github.com/matehaxor03/holistic_json/json"
)


type ProcessorManager struct {
	Start func()
	WakeUp func()
}

func NewProcessorManager(client_manager *dao.ClientManager, domain_name dao.DomainName, port string, queue string, minimum_threads int, maximum_threads int) (*ProcessorManager, []error) {
	var errors []error
	var threads []*Processor

	//status_lock := &sync.Mutex{}
	//var wg sync.WaitGroup
	wakeup_lock := &sync.Mutex{}

	/*
	var this_processor *Processor
	
	var messageCountLock sync.Mutex
	var callbackLock sync.Mutex
	var messageCount uint64
	var processor_function *func(processor *Processor, request *json.Map, response *json.Map) []error*/
	
	/*
	setProcessor := func(processor *Processor) {
		this_processor = processor
	}

	getProcessor := func() *Processor {
		return this_processor
	}
*/
	getQueue := func() string {
		return queue
	}

	getPort := func() string {
		return port
	}

	getClientManager := func() *dao.ClientManager {
		return client_manager
	}
	
	getDomainName := func() dao.DomainName {
		return domain_name
	}
	
	/*
	processor_callback, processor_callback_errors := NewProcessorCallback(domain_name, port)
	if processor_callback_errors != nil {
		return nil, processor_callback_errors
	} else if common.IsNil(processor_callback) {
		errors = append(errors, fmt.Errorf("callback processor is nil"))
		return nil, errors
	}
	processor_callback.Start()
*/
	/*
	getCallbackProcessor := func() *ProcessorCallback {
		return processor_callback
	}*/

	domain_name_value := domain_name.GetDomainName()
	queue_url := fmt.Sprintf("https://%s:%s/", domain_name_value, port)
	
	/*transport_config := &http.Transport{
		TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
	}*/

	/*
	http_client := http.Client{
		Timeout: 120 * time.Second,
		Transport: transport_config,
	}*/

	/*
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
*/
/*
	incrementMessageCount := func() uint64 {
		messageCountLock.Lock()
		defer messageCountLock.Unlock()
		messageCount++
		return messageCount
	}

	generate_guid := func() string {
		byte_array := make([]byte, 16)
		rand.Read(byte_array)
		guid := fmt.Sprintf("%X-%X-%X-%X-%X", byte_array[0:4], byte_array[4:6], byte_array[6:8], byte_array[8:10], byte_array[10:])
		return guid
	}

	generate_trace_id := func() string {
		return fmt.Sprintf("%v-%s-%d", time.Now().UnixNano(), generate_guid(), incrementMessageCount())
	}

	sendMessageToQueueFireAndForget := func (message *json.Map) {
		callbackLock.Lock()
		defer callbackLock.Unlock()
		c := getCallbackProcessor()
		c.SendMessageToQueueFireAndForget(message)
	}

	sendMessageToQueue := func(message *json.Map) (*json.Map, []error) {
		callbackLock.Lock()
		defer callbackLock.Unlock()
		c := getCallbackProcessor()
		return c.SendMessageToQueue(message)
	}*/

	x := ProcessorManager{
		WakeUp: func() {
			wakeup_lock.Lock()
			defer wakeup_lock.Unlock()
			for _, current_processor := range threads {
				current_processor.WakeUp()
			}
		},
		/*
		GetQueue: func() string {
			return getQueue()
		},
		GenerateTraceId: func() string {
			return generate_trace_id()
		},
		GetClientRead: func() *dao.Client {
			return read_database_client
		},
		GetClientWrite: func() *dao.Client {
			return write_database_client
		},
		GetProcessor: func() *Processor {
			return getProcessor()
		},
		SendMessageToQueueFireAndForget: func(message *json.Map) {
			sendMessageToQueueFireAndForget(message)
		},
		SendMessageToQueue: func(message *json.Map) (*json.Map, []error) {
			return sendMessageToQueue(message)
		},
		*/
		Start: func() {
			go func(queue_url string, queue string) {
				for {
					current_number_of_threads := len(threads)
					if current_number_of_threads < minimum_threads {
						difference := minimum_threads - current_number_of_threads
						current_count := 0
						for current_count < difference {
							new_processor, new_processor_errors := NewProcessor(getClientManager(), getDomainName(), getPort(), getQueue())
							if new_processor_errors != nil { 
								fmt.Println(new_processor_errors)
								break
							} else if common.IsNil(new_processor) {
								fmt.Println("spawned processor is nil")
								break
							} else {
								threads = append(threads, new_processor)
								new_processor.Start()
								current_count++
							}
						}
					}

					// check for cpu and memory here for now assume there is enough


					// check if number of items in queue is greater than 0

					current_number_of_threads = len(threads)
					if maximum_threads == -1 && current_number_of_threads < 10 {
						new_processor, new_processor_errors := NewProcessor(getClientManager(), getDomainName(), getPort(), getQueue())
						if new_processor_errors != nil { 
							fmt.Println(new_processor_errors)
							break
						} else if common.IsNil(new_processor) {
							fmt.Println("spawned processor is nil")
							break
						} else {
							threads = append(threads, new_processor)
							new_processor.Start()
						}
					}

					time.Sleep(30 * time.Second) 
				}
			}(queue_url, queue)
		},
	}
	//setProcessor(&x)
	

	if len(errors) > 0 {
		return nil, errors
	}

	return &x, nil
}