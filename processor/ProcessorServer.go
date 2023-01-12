package processor

import (
	"net/http"
	"fmt"
	"io/ioutil"
	common "github.com/matehaxor03/holistic_common/common"
	dao "github.com/matehaxor03/holistic_db_client/dao"
	json "github.com/matehaxor03/holistic_json/json"
	http_extension "github.com/matehaxor03/holistic_http/http_extension"
	helper "github.com/matehaxor03/holistic_db_client/helper"
	validate "github.com/matehaxor03/holistic_db_client/validate"
)

type ProcessorServer struct {
	Start   func() ([]error)
}

func NewProcessorServer(port string, server_crt_path string, server_key_path string, queue_domain_name string, queue_port string) (*ProcessorServer, []error) {
	var errors []error
	verify := validate.NewValidator()

	client_manager, client_manager_errors := dao.NewClientManager()
	if client_manager_errors != nil {
		return nil, client_manager_errors
	}
	
	test_read_client, test_read_client_errors := client_manager.GetClient("127.0.0.1", "3306", "holistic", "holistic_read")
	if test_read_client_errors != nil {
		return nil, test_read_client_errors
	}
	
	test_read_database := test_read_client.GetDatabase()
	
	processors := make(map[string](*Processor))
	table_names, table_names_errors := test_read_database.GetTableNames()
	if table_names_errors != nil {
		return nil, table_names_errors
	}

	domain_name, domain_name_errors := dao.NewDomainName(verify, queue_domain_name)
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

	
	data := json.NewMapValue()
	data.SetMapValue("[fields]", json.NewMapValue())
	data.SetMapValue("[schema]", json.NewMapValue())

	map_system_fields := json.NewMapValue()
	map_system_fields.SetObjectForMap("[port]", port)
	map_system_fields.SetObjectForMap("[server_crt_path]", server_crt_path)
	map_system_fields.SetObjectForMap("[server_key_path]", server_key_path)
	map_system_fields.SetObjectForMap("[queue_port]", queue_port)
	map_system_fields.SetObjectForMap("[queue_domain_name]", queue_domain_name)
	data.SetMapValue("[system_fields]", map_system_fields)

	///

	//todo: add filters to fields

	map_system_schema := json.NewMapValue()
	
	map_port := json.NewMapValue()
	map_port.SetStringValue("type", "string")
	map_system_schema.SetMapValue("[port]", map_port)

	map_server_crt_path := json.NewMapValue()
	map_server_crt_path.SetStringValue("type", "string")
	map_system_schema.SetMapValue("[server_crt_path]", map_server_crt_path)

	map_server_key_path := json.NewMapValue()
	map_server_key_path.SetStringValue("type", "string")
	map_system_schema.SetMapValue("[server_key_path]", map_server_key_path)

	map_queue_port := json.NewMapValue()
	map_queue_port.SetStringValue("type", "string")
	map_system_schema.SetMapValue("[server_key_path]", map_queue_port)

	map_queue_domain_name := json.NewMapValue()
	map_queue_domain_name.SetStringValue("type", "string")
	map_system_schema.SetMapValue("[queue_domain_name]", map_queue_domain_name)

	data.SetMapValue("[system_schema]", map_system_schema)

	getData := func() *json.Map {
		return &data
	}

	getPort := func() (string, []error) {
		temp_value, temp_value_errors := helper.GetField(*getData(), "[system_schema]", "[system_fields]", "[port]", "string")
		if temp_value_errors != nil {
			return "",temp_value_errors
		}
		return temp_value.(string), nil
	}

	getServerCrtPath := func() (string, []error) {
		temp_value, temp_value_errors := helper.GetField(*getData(), "[system_schema]", "[system_fields]", "[server_crt_path]", "string")
		if temp_value_errors != nil {
			return "",temp_value_errors
		}
		return temp_value.(string), nil
	}

	getServerKeyPath := func() (string, []error) {
		temp_value, temp_value_errors := helper.GetField(*getData(), "[system_schema]", "[system_fields]", "[server_key_path]", "string")
		if temp_value_errors != nil {
			return "",temp_value_errors
		}
		return temp_value.(string), nil
	}

	validate := func() []error {
		return dao.ValidateData(getData(), "ProcessorServer")
	}

	/*
	setHolisticQueueServer := func(holisic_queue_server *HolisticQueueServer) {
		this_holisic_queue_server = holisic_queue_server
	}*/

	/*
	getHolisticQueueServer := func() *HolisticQueueServer {
		return this_holisic_queue_server
	}*/

	processRequest := func(w http.ResponseWriter, req *http.Request) {
		var errors []error

		if !(req.Method == "POST" || req.Method == "PATCH" || req.Method == "PUT") {
			errors = append(errors, fmt.Errorf("request method not supported: " + req.Method))
		}

		dummy_result := json.NewMapValue()

		if len(errors) > 0 {
			http_extension.WriteResponse(w, dummy_result, errors)
			return
		}

		body_payload, body_payload_error := ioutil.ReadAll(req.Body);
		if body_payload_error != nil {
			errors = append(errors, body_payload_error)
		} 

		if len(errors) > 0 {
			http_extension.WriteResponse(w, dummy_result, errors)
			return
		}
		
		json_payload, json_payload_errors := json.Parse(string(body_payload))
		if json_payload_errors != nil {
			errors = append(errors, json_payload_errors...)
		} 

		if len(errors) > 0 {
			http_extension.WriteResponse(w, dummy_result, errors)
			return
		}

		queue, queue_errors := json_payload.GetString("[queue]")
		if queue_errors != nil {
			http_extension.WriteResponse(w, dummy_result, queue_errors)
			return
		} else if common.IsNil(queue) {
			queue_errors = append(queue_errors, fmt.Errorf("[queue] is nil"))
			http_extension.WriteResponse(w, dummy_result, queue_errors)
			return
		}

		processor, ok := processors[*queue]
		if !ok {
			errors = append(errors, fmt.Errorf("prcoessor: %s does not exist", *queue))
		}

		if len(errors) > 0 {
			http_extension.WriteResponse(w, dummy_result, errors)
			return
		}


		result_map := map[string]interface{}{"[queue]":*queue}
		result := json.NewMapOfValues(&result_map)

		queue_mode, queue_mode_errors := json_payload.GetString("[queue_mode]")

		if queue_mode_errors != nil {
			errors = append(errors, queue_mode_errors...)
		} 
		
		if common.IsNil(queue_mode) {
			errors = append(errors, fmt.Errorf("[queue_mode] is nil"))
		}

		if !(common.IsNil(queue_mode)) {
			result.SetStringValue("[queue_mode]", *queue_mode)
		}

		trace_id, trace_id_errors := json_payload.GetString("[trace_id]")

		if trace_id_errors != nil {
			errors = append(errors, trace_id_errors...)
		} 
		
		if common.IsNil(trace_id) {
			errors = append(errors, fmt.Errorf("[trace_id] is nil"))
		}

		if !(common.IsNil(trace_id)) {
			result.SetStringValue("[trace_id]", *trace_id)
		}
		
		if len(errors) > 0 {
			http_extension.WriteResponse(w, *result, errors)
			return
		}

		if *queue_mode == "WakeUp" {
			processor.WakeUp()
		} else {
			errors = append(errors, fmt.Errorf("[queue_mode] is not supported: %s",  *queue_mode))
		}

		http_extension.WriteResponse(w, *result, errors)
	}

	x := ProcessorServer{
		Start: func() []error {
			var errors []error

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


	for _, table_name := range table_names {
		create_processor, create_processor_errors := NewProcessor(client_manager, *domain_name, queue_port, "CreateRecords_" + table_name)
		if create_processor_errors != nil {
			errors = append(errors, create_processor_errors...)
		} else if create_processor != nil {
			processors["CreateRecords_" + table_name] = create_processor
		}

		create_record_processor, create_record_processor_errors := NewProcessor(client_manager, *domain_name, queue_port, "CreateRecord_" + table_name)
		if create_record_processor_errors != nil {
			errors = append(errors, create_record_processor_errors...)
		} else if create_record_processor != nil {
			processors["CreateRecord_" + table_name] = create_record_processor
		}

		read_processor, read_processor_errors := NewProcessor(client_manager, *domain_name, queue_port, "ReadRecords_" + table_name)
		if read_processor_errors != nil {
			errors = append(errors, read_processor_errors...)
		} else if read_processor != nil {
			processors["ReadRecords_" + table_name] = read_processor
		}

		update_processor, update_processor_errors := NewProcessor(client_manager, *domain_name, queue_port, "UpdateRecords_" + table_name)
		if update_processor_errors != nil {
			errors = append(errors, update_processor_errors...)
		} else if update_processor != nil {
			processors["UpdateRecords_" + table_name] = update_processor
		}

		update_record_processor, update_record_processor_errors := NewProcessor(client_manager, *domain_name, queue_port, "UpdateRecord_" + table_name)
		if update_record_processor_errors != nil {
			errors = append(errors, update_record_processor_errors...)
		} else if update_record_processor != nil {
			processors["UpdateRecord_" + table_name] = update_record_processor
		}

		/*
		delete_processor, delete_processor_errors := NewProcessor(client_manager, *domain_name, queue_port, "DeleteRecords_" + table_name)
		if delete_processor_errors != nil {
			errors = append(errors, delete_processor_errors...)
		} else if delete_processor != nil {
			processors["DeleteRecords_" + table_name] = delete_processor
		}*/

		get_schema_processor, get_schema_processor_errors := NewProcessor(client_manager, *domain_name, queue_port, "GetSchema_" + table_name)
		if get_schema_processor_errors != nil {
			errors = append(errors, get_schema_processor_errors...)
		} else if get_schema_processor != nil {
			processors["GetSchema_" + table_name] = get_schema_processor
		}
	}

	commands_list := [...]string{"Run_Sync",
								"Run_StartBuildBranchInstance", 
								"Run_NotStarted", 
								"Run_Start",
								"Run_CreateSourceFolder",
								"Run_CreateDomainNameFolder",
								"Run_CreateRepositoryAccountFolder",
								"Run_CreateRepositoryFolder",
								"Run_CreateBranchesFolder",
								"Run_CreateTagsFolder",
								"Run_CreateBranchOrTagFolder",
								"Run_CloneBranchOrTagFolder",
								"Run_PullLatestBranchOrTagFolder",
								"Run_CreateBranchInstancesFolder",
								"Run_CreateTagInstancesFolder",
								"Run_CopyToInstanceFolder",
								"Run_CreateInstanceFolder",
								"Run_CreateGroup",
								"Run_CreateUser",
								"Run_AssignGroupToUser",
								"Run_AssignGroupToInstanceFolder",
								"Run_Clean",
								"Run_Lint",
								"Run_Build",
								"Run_UnitTests",
								"Run_IntegrationTests",
								"Run_IntegrationTestSuite",
								"Run_RemoveGroupFromInstanceFolder",
								"Run_RemoveGroupFromUser",
								"Run_DeleteGroup",
								"Run_DeleteUser",
								"Run_DeleteInstanceFolder",
								"Run_End",
								"GetTableNames"}


	for _, command := range commands_list {
		processor, processor_errors := NewProcessor(client_manager, *domain_name, queue_port, command)
		if processor_errors != nil {
			errors = append(errors, processor_errors...)
		} else if processor != nil {
			processors[command] = processor
		} else {
			errors = append(errors, fmt.Errorf("failed to create processor: %s", command))
		}
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
