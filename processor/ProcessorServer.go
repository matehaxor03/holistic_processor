package queue

import (
	"net/http"
	"fmt"
	"strings"
	"encoding/json"
	"io/ioutil"
	"sync"
	class "github.com/matehaxor03/holistic_db_client/class"
)

type ProcessorServer struct {
	Start   func() ([]error)
}

func NewProcessorServer(port string, server_crt_path string, server_key_path string, queue_domain_name string, queue_port string) (*QueueServer, []error) {
	var errors []error
	wait_groups := make(map[string]sync.WaitGroup)
	//var this_holisic_queue_server *HolisticQueueServer
	
	db_hostname, db_port_number, db_name, read_db_username, _, migration_details_errors := class.GetCredentialDetails("holistic_read")
	if migration_details_errors != nil {
		errors = append(errors, migration_details_errors...)
	}

	if len(errors) > 0 {
		return nil, errors
	}

	host, host_errors := class.NewHost(db_hostname, db_port_number)
	client, client_errors := class.NewClient(host, &read_db_username, nil)

	if host_errors != nil {
		errors = append(errors, host_errors...)
	}

	if client_errors != nil {
		errors = append(errors, client_errors...)
	}

	if len(errors) > 0 {
		return nil, errors
	}

	database, use_database_errors := client.UseDatabaseByName(db_name)
	if use_database_errors != nil {
		return nil, use_database_errors
	}
	
	processors := make(map[string](*Processor))
	table_names, table_names_errors := database.GetTableNames()
	if table_names_errors != nil {
		return nil, table_names_errors
	}

	transport_config := &http.Transport{
		TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
	}

	http_client := http.Client{
		Timeout: 120 * time.Second,
		Transport: transport_config,
	}

	domain_name, domain_name_errors := class.NewDomainName(&queue_domain_name)
	if domain_name_errors != nil {
		errors = append(errors, domain_name_errors...)
	}


	//todo: add filters to fields
	data := class.Map{
		"[port]": class.Map{"value": class.CloneString(&port), "mandatory": true},
		"[server_crt_path]": class.Map{"value": class.CloneString(&server_crt_path), "mandatory": true},
		"[server_key_path]": class.Map{"value": class.CloneString(&server_key_path), "mandatory": true},
		"[queue_port]": class.Map{"value": class.CloneString(&queue_port), "mandatory": true},
		"[queue_domain_name]": class.Map{"value": class.CloneDomainName(domain_name), "mandatory": true},
	}

	getPort := func() *string {
		port, _ := data.M("[port]").GetString("value")
		return class.CloneString(port)
	}

	getServerCrtPath := func() *string {
		crt, _ := data.M("[server_crt_path]").GetString("value")
		return class.CloneString(crt)
	}

	getServerKeyPath := func() *string {
		key, _ := data.M("[server_key_path]").GetString("value")
		return class.CloneString(key)
	}

	getQueuePort := func() *string {
		port, _ := data.M("[queue_port]").GetString("value")
		return class.CloneString(port)
	}

	getQueueDomainName := func() *class.DomainName {
		return class.CloneDomainName(data.M("[queue_domain_name]").GetObject("value").(*class.DomainName))
	}

	queue_url := fmt.Sprintf("https://%s:%s/", *(getQueueDomainName().GetDomainName()), *getQueuePort())


	validate := func() []error {
		return class.ValidateData(data, "ProcessorServer")
	}

	/*
	setHolisticQueueServer := func(holisic_queue_server *HolisticQueueServer) {
		this_holisic_queue_server = holisic_queue_server
	}*/

	/*
	getHolisticQueueServer := func() *HolisticQueueServer {
		return this_holisic_queue_server
	}*/

	formatRequest := func(r *http.Request) string {
		var request []string
	
		url := fmt.Sprintf("%v %v %v", r.Method, r.URL, r.Proto)
		request = append(request, url)
		request = append(request, fmt.Sprintf("Host: %v", r.Host))
		for name, headers := range r.Header {
			name = strings.ToLower(name)
			for _, h := range headers {
				request = append(request, fmt.Sprintf("%v: %v", name, h))
			}
		}
	
		if r.Method == "POST" {
			r.ParseForm()
			request = append(request, "\n")
			request = append(request, r.Form.Encode())
		}
	
		return strings.Join(request, "\n")
	}

	processRequest := func(w http.ResponseWriter, req *http.Request) {
		if req.Method == "POST" || req.Method == "PATCH" || req.Method == "PUT" {
			json_payload := class.Map{}
			body_payload, body_payload_error := ioutil.ReadAll(req.Body);
			if body_payload_error != nil {
				w.Write([]byte(body_payload_error.Error()))
			} else {
				json.Unmarshal([]byte(body_payload), &json_payload)
				
				fmt.Println(json_payload.Keys())
				fmt.Println(string(body_payload))

				message_type, message_type_errors := json_payload.GetString("[queue]")
				trace_id, _ := json_payload.GetString("[trace_id]")

				if message_type_errors != nil {
					w.Write([]byte("[queue] does not exist error"))
				} else {
					queue, ok := queues[*message_type]
					if ok {
						queue_mode, queue_mode_errors := json_payload.GetString("[queue_mode]")
						if queue_mode_errors != nil {
							w.Write([]byte("[queue_mode] does not exist error"))
						} else {
							if *queue_mode == "PushBack" {
								var wg sync.WaitGroup
								wg.Add(1)
								wait_groups[*trace_id] = wg
								queue.PushBack(&json_payload)
								wg.Wait()
								w.Write([]byte("ok"))
							} else {
								fmt.Println(fmt.Sprintf("[queue_mode] not supported please implement: %s", *queue_mode))
								w.Write([]byte(fmt.Sprintf("[queue_mode] not supported please implement: %s", *queue_mode)))
							}
						}
					} else {
						fmt.Println(fmt.Sprintf("[queue] not supported please implement: %s", *message_type))
						w.Write([]byte(fmt.Sprintf("[queue] not supported please implement: %s", *message_type)))
					}
				}
			}
		} else {
			w.Write([]byte(formatRequest(req)))
		}
	}

	x := ProcessorServer{
		Start: func() []error {
			var errors []error
			http.HandleFunc("/", processRequest)

			err := http.ListenAndServeTLS(":" + *(getPort()), *(getServerCrtPath()), *(getServerKeyPath()), nil)
			if err != nil {
				errors = append(errors, err)
			}

			for _, table_name := range *table_names {
				processors["Create_" + table_name].Start()
				processors["Read_" + table_name].Start()
				processors["Update_" + table_name].Start()
				processors["Delete_" + table_name].Start()
			}
			processors["Get_Tables"].Start()

			if len(errors) > 0 {
				return errors
			}

			return nil
		},
	}
	//setHolisticQueueServer(&x)


	for _, table_name := range *table_names {
		processors["Create_" + table_name] = NewProcessor(domain_name, queue_port, "Create_" + table_name)
		processors["Read_" + table_name] = NewProcessor(domain_name, queue_port, "Read_" + table_name)
		processors["Update_" + table_name] = NewProcessor(domain_name, queue_port, "Update_" + table_name)
		processors["Delete_" + table_name] = NewProcessor(domain_name, queue_port, "Delete_" + table_name)
	}

	processors["Get_Tables"] = NewProcessor(domain_name, queue_port, "Get_Tables")


	validate_errors := validate()
	if validate_errors != nil {
		errors = append(errors, validate_errors...)
	}

	if len(errors) > 0 {
		return nil, errors
	}

	return &x, nil
}
