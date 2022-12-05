package processor

import (
	"net/http"
	"fmt"
	"strings"
	//"encoding/json"
	"io/ioutil"
	//"sync"
	//"crypto/tls"
	//"time"
	class "github.com/matehaxor03/holistic_db_client/class"
)

type ProcessorServer struct {
	Start   func() ([]error)
}

func NewProcessorServer(port string, server_crt_path string, server_key_path string, queue_domain_name string, queue_port string) (*ProcessorServer, []error) {
	var errors []error
	struct_type := "processor.ProcessorServer"
	//wait_groups := make(map[string]sync.WaitGroup)
	//var this_holisic_queue_server *HolisticQueueServer
	client_manager, client_manager_errors := class.NewClientManager()
	if client_manager_errors != nil {
		return nil, client_manager_errors
	}

	test_read_client, test_read_client_errors := client_manager.GetClient("holistic_db_config:127.0.0.1:3306:holistic:holistic_read")
	if test_read_client_errors != nil {
		return nil, test_read_client_errors
	}
	
	test_read_database, test_read_database_errors := test_read_client.GetDatabase()
	if test_read_database_errors != nil {
		return nil, test_read_database_errors
	}
	
	processors := make(map[string](*Processor))
	table_names, table_names_errors := test_read_database.GetTableNames()
	if table_names_errors != nil {
		return nil, table_names_errors
	}

	domain_name, domain_name_errors := class.NewDomainName(queue_domain_name)
	if domain_name_errors != nil {
		return nil, domain_name_errors
	}

	

	/*
	transport_config := &http.Transport{
		TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
	}

	http_client := http.Client{
		Timeout: 120 * time.Second,
		Transport: transport_config,
	}*/

	
	


	//todo: add filters to fields
	data := class.Map{
		"[fields]": class.Map{},
		"[schema]": class.Map{},
		"[system_fields]": class.Map{
			"[port]":&port,
			"[server_crt_path]":&server_crt_path,
			"[server_key_path]":&server_key_path,
			"[queue_port]":&queue_port,
			"[queue_domain_name]":domain_name,	
		},
		"[system_schema]":class.Map{
			"[port]": class.Map{"type":"string","mandatory": true},
			"[server_crt_path]": class.Map{"type":"string","mandatory": true},
			"[server_key_path]": class.Map{"type":"string", "mandatory": true},
			"[queue_port]": class.Map{"type":"string", "mandatory": true},
			"[queue_domain_name]": class.Map{"type":"*class.DomainName", "mandatory": true},
		},
	}

	getData := func() *class.Map {
		return &data
	}

	getPort := func() (string, []error) {
		temp_value, temp_value_errors := class.GetField(struct_type, getData(), "[system_schema]", "[system_fields]", "[port]", "string")
		if temp_value_errors != nil {
			return "",temp_value_errors
		}
		return temp_value.(string), nil
	}

	getServerCrtPath := func() (string, []error) {
		temp_value, temp_value_errors := class.GetField(struct_type, getData(), "[system_schema]", "[system_fields]", "[server_crt_path]", "string")
		if temp_value_errors != nil {
			return "",temp_value_errors
		}
		return temp_value.(string), nil
	}

	getServerKeyPath := func() (string, []error) {
		temp_value, temp_value_errors := class.GetField(struct_type, getData(), "[system_schema]", "[system_fields]", "[server_key_path]", "string")
		if temp_value_errors != nil {
			return "",temp_value_errors
		}
		return temp_value.(string), nil
	}

	/*
	getQueuePort := func() *string {
		port, _ := data.M("[queue_port]").GetString("value")
		return class.CloneString(port)
	}

	getQueueDomainName := func() *class.DomainName {
		return class.CloneDomainName(data.M("[queue_domain_name]").GetObject("value").(*class.DomainName))
	}*/

	//queue_url := fmt.Sprintf("https://%s:%s/", *(getQueueDomainName().GetDomainName()), *getQueuePort())


	validate := func() []error {
		return class.ValidateData(getData(), "ProcessorServer")
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
		var errors []error
		if req.Method == "POST" || req.Method == "PATCH" || req.Method == "PUT" {
			body_payload, body_payload_error := ioutil.ReadAll(req.Body);
			if body_payload_error != nil {
				w.Write([]byte(body_payload_error.Error()))
			} else {
				json_payload, json_payload_errors := class.ParseJSON(string(body_payload))
				if json_payload_errors != nil {
					w.Write([]byte(fmt.Sprintf("%s", json_payload_errors)))
				} else {
					fmt.Println(json_payload.Keys())
					fmt.Println(string(body_payload))

					queue, queue_errors := json_payload.GetString("[queue]")
					if queue_errors != nil {
						w.Write([]byte(fmt.Sprintf("%s", queue_errors)))
						return
					}
					
					processor, ok := processors[*queue]
					if !ok {
						w.Write([]byte(fmt.Sprintf("prcoessor: %s does not exist", *queue)))
						return
					}
					//trace_id, _ := json_payload.GetString("[trace_id]")

					queue_mode, queue_mode_errors := json_payload.GetString("[queue_mode]")

					if queue_mode_errors != nil {
						w.Write([]byte(fmt.Sprintf("%s", queue_mode_errors)))
						return
					}

					if *queue_mode == "WakeUp" {
						processor.WakeUp()
						json_payload.SetErrors("[errors]", &errors)
						var json_payload_builder strings.Builder
						json_payload_as_string_errors := json_payload.ToJSONString(&json_payload_builder)
						if json_payload_as_string_errors != nil {
							errors = append(errors, json_payload_as_string_errors...)
							w.Write([]byte(fmt.Sprintf("{\"errors\":\"%s\"}", errors)))
						} else {
							w.Write([]byte(json_payload_builder.String()))
						}
					} else {
						w.Write([]byte(fmt.Sprintf("[queue_mode] is not supported: %s", *queue_mode)))
					}
				
						/*
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
						}*/
					
				}
			}
		} else {
			w.Write([]byte(formatRequest(req)))
		}
	}

	x := ProcessorServer{
		Start: func() []error {
			var errors []error

			fmt.Println(len(processors))
			for _, value := range processors {
				value.Start()
			}

			http.HandleFunc("/", processRequest)

			temp_port, temp_port_errors := getPort()
			if temp_port_errors != nil {
				return temp_port_errors
			}

			temp_server_crt_path, temp_server_crt_path_errors := getServerCrtPath()
			if temp_server_crt_path_errors != nil {
				return temp_server_crt_path_errors
			}

			temp_server_key_path, temp_server_key_path_errors := getServerKeyPath()
			if temp_server_key_path_errors != nil {
				return temp_server_key_path_errors
			}

			err := http.ListenAndServeTLS(":" + temp_port, temp_server_crt_path, temp_server_key_path, nil)
			if err != nil {
				errors = append(errors, err)
			}

			if len(errors) > 0 {
				return errors
			}

			return nil
		},
	}
	//setHolisticQueueServer(&x)


	for _, table_name := range *table_names {
		create_processor, create_processor_errors := NewProcessor(client_manager, *domain_name, queue_port, "Create_" + table_name)
		if create_processor_errors != nil {
			errors = append(errors, create_processor_errors...)
		} else if create_processor != nil {
			processors["Create_" + table_name] = create_processor
		}

		read_processor, read_processor_errors := NewProcessor(client_manager, *domain_name, queue_port, "Read_" + table_name)
		if read_processor_errors != nil {
			errors = append(errors, read_processor_errors...)
		} else if read_processor != nil {
			processors["Read_" + table_name] = read_processor
		}

		update_processor, update_processor_errors := NewProcessor(client_manager, *domain_name, queue_port, "Update_" + table_name)
		if update_processor_errors != nil {
			errors = append(errors, update_processor_errors...)
		} else if update_processor != nil {
			processors["Update_" + table_name] = update_processor
		}

		delete_processor, delete_processor_errors := NewProcessor(client_manager, *domain_name, queue_port, "Delete_" + table_name)
		if delete_processor_errors != nil {
			errors = append(errors, delete_processor_errors...)
		} else if delete_processor != nil {
			processors["Delete_" + table_name] = delete_processor
		}

		get_schema_processor, get_schema_processor_errors := NewProcessor(client_manager, *domain_name, queue_port, "GetSchema_" + table_name)
		if get_schema_processor_errors != nil {
			errors = append(errors, get_schema_processor_errors...)
		} else if get_schema_processor != nil {
			processors["GetSchema_" + table_name] = get_schema_processor
		}
	}

	get_tables_processor, get_tables_processor_errors := NewProcessor(client_manager, *domain_name, queue_port, "GetTableNames")
	if get_tables_processor_errors != nil {
		errors = append(errors, get_tables_processor_errors...)
	} else if get_tables_processor != nil {
		processors["GetTableNames"] = get_tables_processor
	}

	validate_errors := validate()
	if validate_errors != nil {
		errors = append(errors, validate_errors...)
	}

	if len(errors) > 0 {
		return nil, errors
	}

	return &x, nil
}
