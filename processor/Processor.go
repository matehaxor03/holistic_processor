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
	WakeUp func()
}

func NewProcessor(client_manager *class.ClientManager, domain_name class.DomainName, port string, queue string) (*Processor, []error) {
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

	read_database_connection_string := "holistic_db_config:127.0.0.1:3306:holistic:holistic_read"
	read_database_client, read_database_client_errors := client_manager.GetClient(read_database_connection_string)
	if read_database_client_errors != nil {
		return nil, read_database_client_errors
	}
	
	_, read_database_errors := read_database_client.GetDatabase()
	if read_database_errors != nil {
		return nil, read_database_errors
	}

	//todo test the connection string before starting

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
					request_payload := json.Map{queue: json.Map{"[trace_id]":trace_id, "queue_mode":"GetAndRemoveFront"}}
					var json_payload_builder strings.Builder
					request_payload_as_string_errors := request_payload.ToJSONString(&json_payload_builder)

					if request_payload_as_string_errors != nil {
						fmt.Println(request_payload_as_string_errors)
						time.Sleep(10 * time.Second) 
						continue
					}

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

					//response_json_payload := json.Map{}
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






						response_json_payload, response_json_payload_errors := json.ParseJSON(string(response_body_payload))
						if response_json_payload_errors != nil {
							fmt.Println(response_json_payload_errors)
							time.Sleep(10 * time.Second) 
							continue
						}
						
						//response_json_payload_string_after, _ := response_json_payload.ToJSONString()
						//fmt.Println(*response_json_payload_string_after)

						keys := response_json_payload.Keys()
						if len(keys) != 1 {
							fmt.Println("keys length is not 1")
							continue
						}

						response_queue := keys[0]
						json_payload_inner, json_payload_inner_errors := response_json_payload.GetMap(response_queue)
						if json_payload_inner_errors != nil {
							fmt.Println(json_payload_inner_errors) 
							continue
						} else if common.IsNil(json_payload_inner) {
							fmt.Println("payload body is nil") 
							continue
						}

						message_trace_id, message_trace_id_errors := json_payload_inner.GetString("[trace_id]")
						if message_trace_id_errors != nil {
							fmt.Println(message_trace_id_errors) 
						} else if message_trace_id == nil {
							fmt.Println("message_trace_id is nil")
						}

						result := json.Map{}
						response_queue_result := json.Map{"[trace_id]":*message_trace_id, "[queue_mode]":"complete"}
						result.SetObject(response_queue, response_queue_result)
						

						if response_queue == "GetTableNames" {
							temp_client, temp_client_errors := client_manager.GetClient(read_database_connection_string)
							if temp_client_errors != nil {
								response_queue_result.SetNil("data")
								response_queue_result.SetErrors("[errors]", &errors)
							} else {
								temp_read_database, temp_read_database_errors := temp_client.GetDatabase()
								if temp_read_database_errors != nil {
									response_queue_result.SetNil("data")
									response_queue_result.SetErrors("[errors]", &errors)
								} else {
									fmt.Println("getting tablenanes")
									table_names, table_name_errors := temp_read_database.GetTableNames()
									
									if table_name_errors != nil {
										fmt.Println(table_name_errors)
										response_queue_result.SetErrors("[errors]", &table_name_errors)
										response_queue_result.SetNil("data")
									} else {
										array, array_errors := json.ToArray(table_names)
										if array_errors != nil {
											errors = append(errors, array_errors...)
											response_queue_result.SetErrors("[errors]", &errors)
											response_queue_result.SetNil("data")
										} else {
											response_queue_result.SetErrors("[errors]", &errors)
											response_queue_result.SetArray("data", array)
										}
									}
								}
							}
 						} else if strings.HasPrefix(response_queue, "GetSchema_") {
							temp_client, temp_client_errors := client_manager.GetClient(read_database_connection_string)
							if temp_client_errors != nil {
								response_queue_result.SetNil("data")
								response_queue_result.SetErrors("[errors]", &errors)
							} else {
								temp_read_database, temp_read_database_errors := temp_client.GetDatabase()
								if temp_read_database_errors != nil {
									response_queue_result.SetNil("data")
									response_queue_result.SetErrors("[errors]", &errors)
								} else {
									fmt.Println("getting schema for " + response_queue)
									_, unsafe_table_name, _ := strings.Cut(response_queue, "_")
									
									table, table_errors := temp_read_database.GetTable(unsafe_table_name)
									if table_errors != nil {
										errors = append(errors, table_errors...)
										response_queue_result.SetNil("data")
										response_queue_result.SetErrors("[errors]", &errors)
									} else if table != nil {
										schema, schema_errors := table.GetSchema()
										if schema_errors != nil {
											errors = append(errors, schema_errors...)
											response_queue_result.SetNil("data")
											response_queue_result.SetErrors("[errors]", &errors)
										} else {
											response_queue_result.SetErrors("[errors]", &errors)
											response_queue_result.SetMap("data", schema)
										}
									} else {
										errors = append(errors, fmt.Errorf("table is nil"))
										response_queue_result.SetNil("data")
										response_queue_result.SetErrors("[errors]", &errors)
									}
								}
							}
						} else if strings.HasPrefix(response_queue, "Read_") {	
							temp_client, temp_client_errors := client_manager.GetClient(read_database_connection_string)
							if temp_client_errors != nil {
								response_queue_result.SetNil("data")
								response_queue_result.SetErrors("[errors]", &errors)
							} else {
								temp_read_database, temp_read_database_errors := temp_client.GetDatabase()
								if temp_read_database_errors != nil {
									response_queue_result.SetNil("data")
									response_queue_result.SetErrors("[errors]", &errors)
								} else {
									fmt.Println("reading table for " + response_queue)
									_, unsafe_table_name, _ := strings.Cut(response_queue, "_")
									
									table, table_errors := temp_read_database.GetTable(unsafe_table_name)
									if table_errors != nil {
										errors = append(errors, table_errors...)
									}

									if len(errors) > 0 {
										response_queue_result.SetNil("data")
										response_queue_result.SetErrors("[errors]", &errors)
									} else {
										records, records_errors := table.ReadRecords(json.Map{}, nil, nil)
										if records_errors != nil {
											errors = append(errors, records_errors...)
											response_queue_result.SetNil("data")
											response_queue_result.SetErrors("[errors]", &errors)
										} else {
											var array_errors []error
											array := json.Array{}
											for _, record := range *records {
												fields_for_record, fields_for_record_error := record.GetFields()
												if fields_for_record_error != nil {
													records_errors = append(records_errors, fields_for_record_error...)
												} else {
													array = append(array, *fields_for_record)
												}		
											}

											if array_errors != nil {
												errors = append(errors, array_errors...)
												response_queue_result.SetErrors("[errors]", &errors)
												response_queue_result.SetNil("data")
											} else {
												response_queue_result.SetErrors("[errors]", &errors)
												response_queue_result.SetArray("data", &array)
											}
										}
									}
								}
							}
						} else {
							fmt.Println("not supported yet" + response_queue)
						}

						var json_payload_builder strings.Builder
						callback_payload_as_string_errors := result.ToJSONString(&json_payload_builder)
						if callback_payload_as_string_errors != nil {
							fmt.Println(callback_payload_as_string_errors)
							time.Sleep(10 * time.Second) 
							continue
						}

						callback_json_bytes := []byte(json_payload_builder.String())
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
		for range time.Tick(time.Second * 15) {
			x.WakeUp()
		}
	}
	go heart_beat()

	return &x, nil
}