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
	class "github.com/matehaxor03/holistic_db_client/class"
)


type Processor struct {
	Start func()
	WakeUp func()
}

func NewProcessor(domain_name class.DomainName, port string, queue string) (*Processor, []error) {
	var errors []error
	var messageCountLock sync.Mutex
	var messageCount uint64
	
	retry_lock := &sync.Mutex{}
	retry_condition := sync.NewCond(retry_lock)


	wakeup_lock := &sync.Mutex{}


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

	read_database, read_database_errors := class.GetDatabase("holistic_read") 
	if read_database_errors != nil {
		errors = append(errors, read_database_errors...)
	}

	if len(errors) > 0 {
		return nil, errors
	}

	use_database_errors := read_database.UseDatabase() 
	if use_database_errors != nil {
		errors = append(errors, use_database_errors...)
	}

	if len(errors) > 0 {
		return nil, errors
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

	x := Processor{
		WakeUp: func() {
			//wg.Done()
			wakeup_lock.Lock()
			defer wakeup_lock.Unlock()
			(*retry_condition).Signal()
		},
		Start: func() {
			fmt.Println("starting processor " + queue)
			go func(queue_url string, queue string) {
				fmt.Println("started processor " + queue)
				for {
					time.Sleep(1 * time.Nanosecond) 
					var errors []error
					trace_id := fmt.Sprintf("%v-%s-%d", time.Now().UnixNano(), generate_guid(), incrementMessageCount())
					request_payload := class.Map{}
					request_payload.SetString("[queue]", &queue)
					request_payload.SetString("[trace_id]", &trace_id)
					queue_mode := "GetAndRemoveFront"
					request_payload.SetString("[queue_mode]", &queue_mode)
					request_payload_as_string, request_payload_as_string_errors := request_payload.ToJSONString()

					if request_payload_as_string_errors != nil {
						fmt.Println(request_payload_as_string_errors)
						time.Sleep(10 * time.Second) 
						continue
					}

					request_json_bytes := []byte(*request_payload_as_string)
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

					//response_json_payload := class.Map{}
					response_body_payload, response_body_payload_error := ioutil.ReadAll(http_response.Body)

					if response_body_payload_error != nil {
						fmt.Println(response_body_payload_error)
						time.Sleep(10 * time.Second) 
						continue
						//todo: go to sleep permantly
						// continue
					}

					if string(response_body_payload) == "{}" {
						fmt.Println("no data to process")
						//wg.Add(1)
						retry_lock.Lock()
						(*retry_condition).Wait()
						retry_lock.Unlock()
						//wg.Wait()
						//time.Sleep(10 * time.Second) 
					} else {
						fmt.Println("processing " + string(response_body_payload))
						response_json_payload, response_json_payload_errors := class.ParseJSON(string(response_body_payload))
						if response_json_payload_errors != nil {
							fmt.Println(response_json_payload_errors)
							time.Sleep(10 * time.Second) 
							continue
						}
						
						//response_json_payload_string_after, _ := response_json_payload.ToJSONString()
						//fmt.Println(*response_json_payload_string_after)

						response_queue, response_queue_errors := response_json_payload.GetString("[queue]")
						if response_queue_errors != nil {
							fmt.Println(response_queue_errors)
						} else if response_queue == nil {
							fmt.Println("response_queue is nil")
						}

						message_trace_id, message_trace_id_errors := response_json_payload.GetString("[trace_id]")
						if message_trace_id_errors != nil {
							fmt.Println(message_trace_id_errors) 
						} else if message_trace_id == nil {
							fmt.Println("message_trace_id is nil")
						}

						result := class.Map{}
						result.SetString("[trace_id]", message_trace_id)
						result.SetString("[queue]", response_queue)
						complete_queue_mode := "complete"
						result.SetString("[queue_mode]", &complete_queue_mode)

						if *response_queue == "GetTableNames" {
							fmt.Println("getting tablenanes")
							table_names, table_name_errors := read_database.GetTableNames()
							
							if table_name_errors != nil {
								fmt.Println(table_name_errors)
								result.SetErrors("[errors]", &table_name_errors)
								result.SetNil("data")
							} else {
								array, array_errors := class.ToArray(table_names)
								if array_errors != nil {
									errors = append(errors, array_errors...)
									result.SetErrors("[errors]", &errors)
									result.SetNil("data")
								} else {
									result.SetErrors("[errors]", &errors)
									result.SetArray("data", array)
								}
							}
 						} else if strings.HasPrefix(*response_queue, "GetSchema_") {
							fmt.Println("getting schema for " + *response_queue)
							_, unsafe_table_name, _ := strings.Cut(*response_queue, "_")
							
							table, table_errors := read_database.GetTable(unsafe_table_name)
							if table_errors != nil {
								errors = append(errors, table_errors...)
								result.SetNil("data")
								result.SetErrors("[errors]", &errors)
							} else if table != nil {
								schema, schema_errors := table.GetSchema()
								if schema_errors != nil {
									errors = append(errors, schema_errors...)
									result.SetNil("data")
									result.SetErrors("[errors]", &errors)
								} else {
									result.SetErrors("[errors]", &errors)
									result.SetMap("data", schema)
								}
							} else {
								errors = append(errors, fmt.Errorf("table is nil"))
								result.SetNil("data")
								result.SetErrors("[errors]", &errors)
							}

						} else if strings.HasPrefix(*response_queue, "Read_") {
							fmt.Println("reading table for " + *response_queue)
							_, unsafe_table_name, _ := strings.Cut(*response_queue, "_")
							
							table, table_errors := read_database.GetTable(unsafe_table_name)
							if table_errors != nil {
								errors = append(errors, table_errors...)
							}

							if len(errors) > 0 {
								result.SetNil("data")
								result.SetErrors("[errors]", &errors)
							} else {
								records, records_errors := table.Select(class.Map{}, nil, nil)
								if records_errors != nil {
									errors = append(errors, records_errors...)
									result.SetNil("data")
									result.SetErrors("[errors]", &errors)
								} else {
									array, array_errors := class.ToArray(records)
									if array_errors != nil {
										errors = append(errors, array_errors...)
										result.SetErrors("[errors]", &errors)
										result.SetNil("data")
									} else {
										result.SetErrors("[errors]", &errors)
										result.SetArray("data", array)
									}
								}
							}
						} else {
							fmt.Println("not supported yet" + *response_queue)
						}

						callback_payload_as_string, callback_payload_as_string_errors := result.ToJSONString()
						if callback_payload_as_string_errors != nil {
							fmt.Println(callback_payload_as_string_errors)
							time.Sleep(10 * time.Second) 
							continue
						}

						callback_json_bytes := []byte(*callback_payload_as_string)
						callback_json_reader := bytes.NewReader(callback_json_bytes)
						callback_request, callback_request_error := http.NewRequest(http.MethodPost, queue_url, callback_json_reader)

						if callback_request_error != nil {
							fmt.Println(callback_request_error)
							time.Sleep(10 * time.Second) 
							continue

							//todo: go to sleep permantly
							// continue
						}

						http_callback_response, http_callback_response_error := http_client.Do(callback_request)
						if http_callback_response_error != nil {
							fmt.Println(http_callback_response_error)
							time.Sleep(10 * time.Second) 
							continue
							//todo: go to sleep permantly
							// continue
						}


						callback_response_body_payload, callback_response_body_payload_error := ioutil.ReadAll(http_callback_response.Body)

						if callback_response_body_payload_error != nil {
							fmt.Println(callback_response_body_payload_error)
							time.Sleep(10 * time.Second) 
							continue
							//todo: go to sleep permantly
							// continue
						}

						fmt.Println("callback response: " +  string(callback_response_body_payload))


						//dowork
						//go sleep for short time
						
					}
				}
			}(queue_url, queue)
		},
	}

	if len(errors) > 0 {
		return nil, errors
	}
	
	heart_beat := func() {
		for range time.Tick(time.Second * 30) {
			x.WakeUp()
		}
	}
	go heart_beat()

	return &x, nil
}