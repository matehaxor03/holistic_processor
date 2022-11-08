package processor

import (
	//"sync"
	"fmt"
	"io/ioutil"
	"net/http"
	"bytes"
	"crypto/tls"
	"time"
	"encoding/json"
	class "github.com/matehaxor03/holistic_db_client/class"
)


type Processor struct {
	Start func()
}

func NewProcessor(domain_name class.DomainName, port string, queue string) (*Processor, []error) {
	var errors []error
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
		Start: func() {
			go func(queue_url string, queue string) {
				for {
					request_payload := class.Map{}
					request_payload.SetString("[queue]", &queue)
					queue_mode := "GetAndRemoveFront"
					request_payload.SetString("[queue_mode]", &queue_mode)
					request_json_bytes := []byte(request_payload.ToJSONString())
					request_json_reader := bytes.NewReader(request_json_bytes)

					request, request_error := http.NewRequest(http.MethodPost, queue_url, request_json_reader)
					if request_error != nil {
						//todo: go to sleep permantly
						// continue
					}
					
					request.Header.Set("Content-Type", "application/json")
					http_response, http_response_error := http_client.Do(request)
					if http_response_error != nil {
						//todo: go to sleep permantly
						// continue
					}

					response_json_payload := class.Map{}
					response_body_payload, response_body_payload_error := ioutil.ReadAll(http_response.Body)

					if response_body_payload_error != nil {
						//todo: go to sleep permantly
						// continue
					}

					json.Unmarshal([]byte(response_body_payload), &response_json_payload)

					if string(response_body_payload) == "{}" {
						//todo: go to sleep permantly
					} else {
						response_queue, _ := response_json_payload.GetString("[queue]")
						trace_id, _ := response_json_payload.GetString("[trace_id]")

						result := class.Map{}
						result.SetString("[trace_id]", trace_id)
						result.SetString("[queue]", response_queue)
						complete_queue_mode := "complete"
						result.SetString("[queue_mode]", &complete_queue_mode)

						if *response_queue == "GetTableNames" {
							table_names, table_name_errors := read_database.GetTableNames()
							result.SetArray("data", class.NewArrayOfStrings(table_names))
							result.SetErrors("errors", table_name_errors)
						} else {

						}

						callback_json_bytes := []byte(result.ToJSONString())
						callback_json_reader := bytes.NewReader(callback_json_bytes)
						callback_request, callback_request_error := http.NewRequest(http.MethodPost, queue_url, callback_json_reader)

						if callback_request_error != nil {
							//todo: go to sleep permantly
							// continue
						}

						_, http_callback_response_error := http_client.Do(callback_request)
						if http_callback_response_error != nil {
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