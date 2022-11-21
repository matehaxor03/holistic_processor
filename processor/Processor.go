package processor

import (
	//"sync"
	"fmt"
	"io/ioutil"
	"net/http"
	"bytes"
	"crypto/tls"
	"time"
	"sync"
	"strings"
	//"encoding/json"
	class "github.com/matehaxor03/holistic_db_client/class"
)


type Processor struct {
	Start func()
	WakeUp func()
}

func NewProcessor(domain_name class.DomainName, port string, queue string) (*Processor, []error) {
	var errors []error
	var wg sync.WaitGroup

	queue_url := fmt.Sprintf("https://%s:%s/", *(domain_name.GetDomainName()), port)
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

	x := Processor{
		WakeUp: func() {
			wg.Done()
		},
		Start: func() {
			fmt.Println("starting processor " + queue)
			go func(queue_url string, queue string) {
				fmt.Println("started processor " + queue)
				for {
					time.Sleep(1 * time.Nanosecond) 
					var errors []error
					request_payload := class.Map{}
					request_payload.SetString("[queue]", &queue)
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

					fmt.Println(string(response_body_payload))

					if string(response_body_payload) == "{}" {
						fmt.Println("no data to process")
						wg.Add(1)
						wg.Wait()
						//time.Sleep(10 * time.Second) 
					} else {
						response_json_payload, response_json_payload_errors := class.ParseJSON(string(response_body_payload))
						if response_json_payload_errors != nil {
							fmt.Println(response_json_payload_errors)
							time.Sleep(10 * time.Second) 
							continue
						}

						response_queue, _ := response_json_payload.GetString("[queue]")
						trace_id, _ := response_json_payload.GetString("[trace_id]")

						result := class.Map{}
						result.SetString("[trace_id]", trace_id)
						result.SetString("[queue]", response_queue)
						complete_queue_mode := "complete"
						result.SetString("[queue_mode]", &complete_queue_mode)

						if *response_queue == "GetTableNames" {
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
						} else if strings.HasPrefix(*response_queue, "Read_") {
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

						_, http_callback_response_error := http_client.Do(callback_request)
						if http_callback_response_error != nil {
							fmt.Println(http_callback_response_error)
							time.Sleep(10 * time.Second) 
							continue
							//todo: go to sleep permantly
							// continue
						}


						//dowork
						//go sleep for short time
						
					}
				}
			}(queue_url, queue)
		},
	}

	return &x, nil
}