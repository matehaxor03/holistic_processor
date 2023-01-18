package processor

import (
	"fmt"
	"time"
	"sync"
	common "github.com/matehaxor03/holistic_common/common"
	dao "github.com/matehaxor03/holistic_db_client/dao"
	json "github.com/matehaxor03/holistic_json/json"
)


type ProcessorManager struct {
	Start func()
	WakeUp func()
	SetQueueCompleteFunction func(*func(json.Map) []error)
	GetQueueCompleteFunction func() (*func(json.Map) []error)
	SetQueueGetNextFunction func(*func(string, string) (json.Map, []error))
	GetQueueGetNextFunction func() (*func(string, string) (json.Map, []error))
	SetQueuePushBackFunction func(*func(string,*json.Map) (*json.Map, []error))
	GetQueuePushBackFunction func() (*func(string,*json.Map) (*json.Map, []error))
	SetProcessorController func(*ProcessorController)
	GetProcessorController func() *ProcessorController
}

func NewProcessorManager(client_manager *dao.ClientManager, domain_name dao.DomainName, queue_port string, queue_name string, minimum_threads int, maximum_threads int) (*ProcessorManager, []error) {
	var this_processor_manager *ProcessorManager
	var processsor_controller *ProcessorController
	var errors []error
	var threads []*Processor
	wakeup_lock := &sync.Mutex{}

	var queue_complete_function (*func(json.Map) []error)
	var queue_get_next_message_function (*func(string, string) (json.Map, []error))
	var queue_push_back_function (*func(string,*json.Map) (*json.Map, []error))

	getQueueName := func() string {
		return queue_name
	}

	getQueuePort := func() string {
		return queue_port
	}

	getClientManager := func() *dao.ClientManager {
		return client_manager
	}
	
	getDomainName := func() dao.DomainName {
		return domain_name
	}

	getProcessorManager := func() *ProcessorManager {
		return this_processor_manager
	}

	setProcessorManager := func(processor_manager *ProcessorManager) {
		this_processor_manager = processor_manager
	}

	domain_name_value := domain_name.GetDomainName()
	queue_url := fmt.Sprintf("https://%s:%s/queue_api/" + getQueueName(), domain_name_value, getQueuePort())

	x := ProcessorManager{
		WakeUp: func() {
			wakeup_lock.Lock()
			defer wakeup_lock.Unlock()
			for _, current_processor := range threads {
				current_processor.WakeUp()
			}
		},
		SetQueueCompleteFunction: func(function *func(json.Map) []error) {
			queue_complete_function = function
		},
		GetQueueCompleteFunction: func() (*func(json.Map) []error) {
			return queue_complete_function
		},
		SetQueueGetNextFunction: func(function *func(string, string) (json.Map, []error)) {
			queue_get_next_message_function = function
		},
		GetQueueGetNextFunction: func() (*func(string, string) (json.Map, []error)) {
			return queue_get_next_message_function
		},
		SetQueuePushBackFunction: func(function *func(string,*json.Map) (*json.Map, []error)) {
			queue_push_back_function = function
		},
		GetQueuePushBackFunction: func() (*func(string,*json.Map) (*json.Map, []error)) {
			return queue_push_back_function
		},
		Start: func() {
			go func(queue_url string, queue_name string) {
				for {
					current_number_of_threads := len(threads)
					if current_number_of_threads < minimum_threads {
						difference := minimum_threads - current_number_of_threads
						current_count := 0
						for current_count < difference {
							new_processor, new_processor_errors := NewProcessor(getClientManager(), getProcessorManager(), getDomainName(), getQueuePort(), getQueueName())
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
						new_processor, new_processor_errors := NewProcessor(getClientManager(), getProcessorManager(), getDomainName(), getQueuePort(), getQueueName())
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
			}(queue_url, queue_name)
		},
		SetProcessorController: func(value *ProcessorController) {
			processsor_controller = value
		},
		GetProcessorController: func() *ProcessorController {
			return processsor_controller
		},
	}
	setProcessorManager(&x)
	
	if len(errors) > 0 {
		return nil, errors
	}

	return &x, nil
}