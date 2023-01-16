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
}

func NewProcessorManager(complete_function (*func(json.Map) []error), get_next_message_function (*func(string, string) (json.Map, []error)), push_back_function (*func(string,*json.Map) (*json.Map, []error)), client_manager *dao.ClientManager, domain_name dao.DomainName, port string, queue string, minimum_threads int, maximum_threads int) (*ProcessorManager, []error) {
	var errors []error
	var threads []*Processor
	wakeup_lock := &sync.Mutex{}

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

	domain_name_value := domain_name.GetDomainName()
	queue_url := fmt.Sprintf("https://%s:%s/queue_api", domain_name_value, port)

	x := ProcessorManager{
		WakeUp: func() {
			wakeup_lock.Lock()
			defer wakeup_lock.Unlock()
			for _, current_processor := range threads {
				current_processor.WakeUp()
			}
		},
		Start: func() {
			go func(queue_url string, queue string) {
				for {
					current_number_of_threads := len(threads)
					if current_number_of_threads < minimum_threads {
						difference := minimum_threads - current_number_of_threads
						current_count := 0
						for current_count < difference {
							new_processor, new_processor_errors := NewProcessor(complete_function, get_next_message_function, push_back_function, getClientManager(), getDomainName(), getPort(), getQueue())
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
						new_processor, new_processor_errors := NewProcessor(complete_function, get_next_message_function, push_back_function, getClientManager(), getDomainName(), getPort(), getQueue())
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
	
	if len(errors) > 0 {
		return nil, errors
	}

	return &x, nil
}