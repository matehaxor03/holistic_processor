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
	"crypto/rand"
	common "github.com/matehaxor03/holistic_common/common"
	class "github.com/matehaxor03/holistic_db_client/class"
	json "github.com/matehaxor03/holistic_json/json"
)


type Processor struct {
	Start func()
	SendMessageToQueue func(message *json.Map) (*json.Map, []error)
	SendMessageToQueueFireAndForget func(message *json.Map) 
	GetProcessor func() *Processor
	GetClientRead func() *class.Client
	GetClientWrite func() *class.Client
	GetQueue func() string
	GenerateTraceId func() string
	WakeUp func()
}

func NewProcessor(client_manager *class.ClientManager, domain_name class.DomainName, port string, queue string) (*Processor, []error) {
	status := "not started"
	status_lock := &sync.Mutex{}
	var wg sync.WaitGroup
	wakeup_lock := &sync.Mutex{}

	var this_processor *Processor
	var errors []error
	var messageCountLock sync.Mutex
	var callbackLock sync.Mutex
	var messageCount uint64
	var processor_function *func(processor *Processor, request *json.Map, response *json.Map) []error
	
	
	setProcessor := func(processor *Processor) {
		this_processor = processor
	}

	getProcessor := func() *Processor {
		return this_processor
	}

	getQueue := func() string {
		return queue
	}

	//retry_lock := &sync.Mutex{}
	//retry_condition := sync.NewCond(retry_lock)
	

	processor_callback, processor_callback_errors := NewProcessorCallback(domain_name, port)
	if processor_callback_errors != nil {
		return nil, processor_callback_errors
	} else if common.IsNil(processor_callback) {
		errors = append(errors, fmt.Errorf("callback processor is nil"))
		return nil, errors
	}
	processor_callback.Start()

	getCallbackProcessor := func() *ProcessorCallback {
		return processor_callback
	}

	//var wg sync.WaitGroup
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

	read_database_connection_string := "holistic_db_config#127.0.0.1#3306#holistic#holistic_read"
	read_database_client, read_database_client_errors := client_manager.GetClient(read_database_connection_string)
	if read_database_client_errors != nil {
		return nil, read_database_client_errors
	}
	
	_, read_database_errors := read_database_client.GetDatabase()
	if read_database_errors != nil {
		return nil, read_database_errors
	}

	write_database_connection_string := "holistic_db_config#127.0.0.1#3306#holistic#holistic_write"
	write_database_client, write_database_client_errors := client_manager.GetClient(write_database_connection_string)
	if write_database_client_errors != nil {
		return nil, write_database_client_errors
	}
	
	_, write_database_errors := write_database_client.GetDatabase()
	if write_database_errors != nil {
		return nil, write_database_errors
	}

	if queue == "GetTableNames" {
		processor_function = commandGetTableNamesFunc()
	} else if strings.HasPrefix(queue, "GetSchema_") {
		processor_function = commandGetSchemaFunc()
	} else if strings.HasPrefix(queue, "ReadRecords_") {
		processor_function = commandReadRecordsFunc()
	} else if strings.HasPrefix(queue, "UpdateRecords_") {
		processor_function = commandUpdateRecordsFunc()
	} else if strings.HasPrefix(queue, "UpdateRecord_") {
		processor_function = commandUpdateRecordFunc()
	} else if strings.HasPrefix(queue, "CreateRecords_") {
		processor_function = commandCreateRecordsFunc()
	} else if strings.HasPrefix(queue, "CreateRecord_") {
		processor_function = commandCreateRecordFunc()
	} else if queue == "Run_StartBuildBranchInstance" {
		processor_function = commandRunStartBuildBranchInstanceFunc()
	} else if queue == "Run_NotStarted" {
		processor_function = commandRunNotStartedFunc()
	} else if queue == "Run_Start" {
		processor_function = commandRunStartFunc()
	} else if queue == "Run_CreateSourceFolder" {
		processor_function = commandRunCreateSourceFolderFunc()
	} else if queue == "Run_CreateDomainNameFolder" {
		processor_function = commandRunCreateDomainNameFolderFunc()
	} else if queue == "Run_CreateRepositoryAccountFolder" {
		processor_function = commandRunCreateRepositoryAccountFolderFunc()
	} else if queue == "Run_CreateRepositoryFolder" {
		processor_function = commandRunCreateRepositoryFolderFunc()
	} else if queue == "Run_CreateBranchesFolder" {
		processor_function = commandRunCreateBranchesFolderFunc()
	} else if queue == "Run_CreateTagsFolder" {
		processor_function = commandRunCreateTagsFolderFunc()
	} else if queue == "Run_CreateBranchOrTagFolder" {
		processor_function = commandRunCreateBranchOrTagFolderFunc()
	} else if queue == "Run_CloneBranchOrTagFolder" {
		processor_function = commandRunCloneBranchOrTagFolderFunc()
	} else if queue == "Run_PullLatestBranchOrTagFolder" {
		processor_function = commandRunPullLatestBranchOrTagFolderFunc()
	} else if queue == "Run_CreateBranchInstancesFolder" {
		processor_function = commandRunCreateBranchInstancesFolderFunc()
	} else if queue == "Run_CreateTagInstancesFolder" {
		processor_function = commandRunCreateTagInstancesFolderFunc()
	} else if queue == "Run_CopyToInstanceFolder" {
		processor_function = commandRunCopyToInstanceFolderFunc()
	} else if queue == "Run_CreateInstanceFolder" {
		processor_function = commandRunCreateInstanceFolderFunc()
	} else if queue == "Run_CreateGroup" {
		processor_function = commandRunCreateGroupFunc()
	} else if queue == "Run_CreateUser" {
		processor_function = commandRunCreateUserFunc()
	} else if queue == "Run_DeleteUser" {
		processor_function = commandRunCreateUserFunc()
	}  else if queue == "Run_DeleteGroup" {
		processor_function = commandRunDeleteGroupFunc()
	} else if queue == "Run_DeleteInstanceFolder" {
		processor_function = commandRunDeleteInstanceFolderFunc()
	} else if queue == "Run_End" {
		processor_function = commandRunEndFunc()
	} else if queue == "Run_Clean" {
		processor_function = commandRunCleanFunc()
	} else if queue == "Run_Lint" {
		processor_function = commandRunLintFunc()
	} else if queue == "Run_RemoveGroupFromInstanceFolder" {
		processor_function = commandRunRemoveGroupFromInstanceFolderFunc()
	} else if queue == "Run_RemoveGroupFromUser" {
		processor_function = commandRunRemoveGroupFromUserFunc()
	} else if queue == "Run_UnitTests" {
		processor_function = commandRunUnitTestsFunc()
	} else if queue == "Run_IntegrationTests" {
		processor_function = commandRunIntegrationTestsFunc()
	} else if queue == "Run_IntegrationTestSuite" {
		processor_function = commandRunIntegrationTestSuiteFunc()
	} else if queue == "Run_Build" {
		processor_function = commandRunBuildFunc()
	} else if queue == "Run_AssignGroupToUser" {
		processor_function = commandRunAssignGroupToUserFunc()
	} else if queue == "Run_AssignGroupToInstanceFolder" {
		processor_function = commandRunAssignGroupToInstanceFolderFunc()
	} else if queue == "Run_Sync" {
		processor_function = commandRunSyncFunc()
	} else {
		errors = append(errors, fmt.Errorf("queue %s processor mapping does not exist", queue))
		return nil, errors
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
	}

	x := Processor{
		WakeUp: func() {
			wakeup_lock.Lock()
			defer wakeup_lock.Unlock()
			if get_or_set_status("")  == "paused" {
				//(*retry_condition).Signal()
				get_or_set_status("try again") 
				wg.Done()
			} else {
				get_or_set_status("try again") 
			}
		},
		GetQueue: func() string {
			return getQueue()
		},
		GenerateTraceId: func() string {
			return generate_trace_id()
		},
		GetClientRead: func() *class.Client {
			return read_database_client
		},
		GetClientWrite: func() *class.Client {
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
		Start: func() {
			fmt.Println("starting processor " + queue)
			go func(queue_url string, queue string) {
				fmt.Println("started processor " + queue)
				for {
					get_or_set_status("running")
					time.Sleep(1 * time.Nanosecond) 
					trace_id := generate_trace_id()
					request_payload := json.Map{"[queue]":queue, "[trace_id]":trace_id, "[queue_mode]":"GetAndRemoveFront"}
					var json_payload_builder strings.Builder
					request_payload_as_string_errors := request_payload.ToJSONString(&json_payload_builder)

					if request_payload_as_string_errors != nil {
						fmt.Println(request_payload_as_string_errors)
						time.Sleep(10 * time.Second) 
						continue
					}

					//fmt.Println(json_payload_builder.String())

					request_json_bytes := []byte(json_payload_builder.String())
					request_json_reader := bytes.NewReader(request_json_bytes)

					request, request_error := http.NewRequest(http.MethodPost, queue_url, request_json_reader)
					if request_error != nil {
						fmt.Println(request_error)
						time.Sleep(10 * time.Second) 
						continue
						//todo: go to sleep permantly
						// continue
					}
					
					request.Header.Set("Content-Type", "application/json")
					http_response, http_response_error := http_client.Do(request)
					if http_response_error != nil {
						fmt.Println(http_response_error)
						time.Sleep(10 * time.Second) 
						continue
						//todo: go to sleep permantly
						// continue
					}

					response_body_payload, response_body_payload_error := ioutil.ReadAll(http_response.Body)

					if response_body_payload_error != nil {
						fmt.Println(response_body_payload_error)
						time.Sleep(10 * time.Second) 
						continue
						//todo: go to sleep permantly
						// continue
					}

					
						//fmt.Println("processing " + string(response_body_payload))


						request_json_payload, request_json_payload_errors := json.ParseJSON(string(response_body_payload))
						if request_json_payload_errors != nil {
							fmt.Println(request_json_payload_errors)
							time.Sleep(10 * time.Second) 
							continue
						}

						response_queue, response_queue_errors := request_json_payload.GetString("[queue]")
						if response_queue_errors != nil {
							fmt.Println(response_queue_errors) 
							continue
						} else if common.IsNil(response_queue) {
							fmt.Println("response_queue is nil")
							continue
						}

						message_trace_id, message_trace_id_errors := request_json_payload.GetString("[trace_id]")
						if message_trace_id_errors != nil {
							fmt.Println(message_trace_id_errors) 
							continue
						} else if message_trace_id == nil {
							fmt.Println("message_trace_id is nil from fetching from queue")
							continue
						}

						async, async_errors := request_json_payload.GetBool("[async]")
						if async_errors != nil {
							fmt.Println(message_trace_id_errors) 
							continue
						} else if common.IsNil(async) {
							fmt.Println("async is nil") 
							continue
						}

						(*request_json_payload)["[queue_mode]"] = "complete"
						(*request_json_payload)["[trace_id]"] = *message_trace_id
						result := json.Map{"[queue]":*response_queue, "[trace_id]":*message_trace_id, "[queue_mode]":"complete", "[async]":*async}
				
						if *response_queue == "empty" {
							// todo get length
							if  get_or_set_status("") == "running" {
								wg.Add(1)
								get_or_set_status("paused")
								wg.Wait()
								get_or_set_status("running")

								

								/*retry_lock.Lock()
								(*retry_condition).Wait()
								retry_lock.Unlock() */
							}
						}

						if *response_queue == "empty" {
							continue
						}

						processor_errors := (*processor_function)(getProcessor(), request_json_payload, &result)
						if processor_errors != nil {
							result.SetNil("data")
							fmt.Println(processor_errors)
							result.SetErrors("[errors]", &processor_errors)
						} else {
							result.SetNil("[errors]")
						}

						if !request_json_payload.IsBoolTrue("[async]") {
							sendMessageToQueueFireAndForget(&result)
						}
					}
				
			}(queue_url, queue)
		},
	}
	setProcessor(&x)
	

	if len(errors) > 0 {
		return nil, errors
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