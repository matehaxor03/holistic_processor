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
	common "github.com/matehaxor03/holistic_common/common"
	class "github.com/matehaxor03/holistic_db_client/class"
	json "github.com/matehaxor03/holistic_json/json"
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
	data := json.Map{
		"[fields]": json.Map{},
		"[schema]": json.Map{},
		"[system_fields]": json.Map{
			"[port]":&port,
			"[server_crt_path]":&server_crt_path,
			"[server_key_path]":&server_key_path,
			"[queue_port]":&queue_port,
			"[queue_domain_name]":domain_name,	
		},
		"[system_schema]":json.Map{
			"[port]": json.Map{"type":"string"},
			"[server_crt_path]": json.Map{"type":"string"},
			"[server_key_path]": json.Map{"type":"string"},
			"[queue_port]": json.Map{"type":"string"},
			"[queue_domain_name]": json.Map{"type":"class.DomainName"},
		},
	}

	getData := func() *json.Map {
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

	write_response := func(w http.ResponseWriter, result json.Map, write_response_errors []error) {
		keys := result.Keys()
		
		if len(keys) != 1 {
			write_response_errors = append(write_response_errors, fmt.Errorf("number of root keys is incorrect"))
		}
		
		if len(write_response_errors) > 0 {
			inner_map_found := false
			if len(keys) == 1 {
				inner_map, inner_map_errors := result.GetMap(keys[0])
				if inner_map_errors != nil {
					write_response_errors = append(write_response_errors, inner_map_errors...)
				} 
				
				if inner_map == nil {
					write_response_errors = append(write_response_errors, fmt.Errorf("inner map is nil"))
					inner_map_found = false
				} else {
					inner_map_found = true
				}
			}

			if inner_map_found {
				(result[keys[0]].(json.Map))["data"] = nil
				(result[keys[0]].(json.Map))["[errors]"] = write_response_errors
			} else {
				result["unknown"] = json.Map{"data":nil, "[errors]":write_response_errors}
			}
		}

		var json_payload_builder strings.Builder
		result_as_string_errors := result.ToJSONString(&json_payload_builder)
		if result_as_string_errors != nil {
			write_response_errors = append(write_response_errors, result_as_string_errors...)
		}
		
		w.Header().Set("Content-Type", "application/json")
		if result_as_string_errors == nil {
			w.Write([]byte(json_payload_builder.String()))
		} else {
			w.Write([]byte(fmt.Sprintf("{\"unknown\":{\"[errors]\":\"%s\", \"data\":null}}", strings.ReplaceAll(fmt.Sprintf("%s", result_as_string_errors), "\"", "\\\""))))
		}
	}

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
				errors = append(errors, body_payload_error)
			} 

			if len(errors) > 0 {
				write_response(w, json.Map{}, errors)
				return
			}
			
			json_payload, json_payload_errors := json.ParseJSON(string(body_payload))
			if json_payload_errors != nil {
				errors = append(errors, json_payload_errors...)
			} 

			if len(errors) > 0 {
				write_response(w, json.Map{}, errors)
				return
			}
		
			fmt.Println(json_payload.Keys())
			fmt.Println(string(body_payload))

			keys := json_payload.Keys()
			if len(keys) != 1 {
				errors = append(errors, fmt.Errorf("keys is not equal to one"))
			}

			if len(errors) > 0 {
				write_response(w, json.Map{}, errors)
				return
			}

			queue := keys[0]
			processor, ok := processors[queue]
			if !ok {
				errors = append(errors, fmt.Errorf("prcoessor: %s does not exist", queue))
			}

			if len(errors) > 0 {
				write_response(w, json.Map{}, errors)
				return
			}

			result := json.Map{queue: json.Map{}}

			json_payload_inner, json_payload_inner_errors := json_payload.GetMap(queue)
			if json_payload_inner_errors != nil {
				errors = append(errors, json_payload_inner_errors...)
			} 
			
			
			if json_payload_inner == nil {
				errors = append(errors, fmt.Errorf("json payload inner is nil"))
			}

			if len(errors) > 0 {
				write_response(w, json.Map{}, errors)
				return
			}
					//trace_id, _ := json_payload.GetString("[trace_id]")

			queue_mode, queue_mode_errors := json_payload_inner.GetString("[queue_mode]")

			if queue_mode_errors != nil {
				errors = append(errors, queue_mode_errors...)
			} 
			
			if common.IsNil(queue_mode) {
				errors = append(errors, fmt.Errorf("[queue_mode] is nil"))
			}

			if !(common.IsNil(queue_mode)) {
				(result[queue].(json.Map))["[queue_mode]"] = *queue_mode
			}

			trace_id, trace_id_errors := json_payload_inner.GetString("[trace_id]")

			if trace_id_errors != nil {
				errors = append(errors, trace_id_errors...)
			} 
			
			if common.IsNil(trace_id) {
				errors = append(errors, fmt.Errorf("[trace_id]  is nil"))
			}

			if !(common.IsNil(trace_id)) {
				(result[queue].(json.Map))["[trace_id]"] = *trace_id
			}
			
			if len(errors) > 0 {
				write_response(w, result, errors)
				return
			}

			if *queue_mode == "WakeUp" {
				processor.WakeUp()
			} else {
				errors = append(errors, fmt.Errorf("[queue_mode] is not supported: %s",  *queue_mode))
			}

			write_response(w, result, errors)
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
