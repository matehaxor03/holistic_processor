package queue

import (
	"container/list"
	"sync"
	class "github.com/matehaxor03/holistic_db_client/class"
)


type Processor struct {
	Start func()
}

func NewProcessor(domain_name class.DomainName, port string, queue string) (*Processor) {
	queue_url := fmt.Sprintf("https://%s:%s/", *(domain_name.GetDomainName()), port)

	var lock sync.Mutex

	x := Processor{
		Start: func() {
			go func(queue_url string, queue string) {
				for {
					request_payload := class.Map{}
					request_payload.SetString("[queue]", queue)
					request_payload.SetString("[queue_mode]", "GetAndRemoveFront")
					request_json_bytes := []byte(request_payload.ToJSONString())
					request_json_reader := bytes.NewReader(request_json_bytes)

					request, request_error := http.NewRequest(http.MethodPost, queue_url, request_json_reader)
					request.Header.Set("Content-Type", "application/json")
					http_response, http_response_error := http_client.Do(request)

					response_json_payload := class.Map{}
					response_body_payload, response_body_payload_error := ioutil.ReadAll(http_response.Body)
					json.Unmarshal([]byte(response_body_payload), &response_json_payload)

					if response_body_payload == "{}" {
						//go to sleep permantly
					} else {
						response_queue, _ := response_json_payload.GetString("[queue]")
						response_queue_mode, _ := response_json_payload.GetString("[queue_mode]")
						if response_queue == "Get_Tables" {

						} else {
							
						}
						//dowork
						//go sleep for short time

					}
				}
			}(queue_url, queue)
		},
	}

	return &x
}