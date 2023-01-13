package processor

import (
	"net/http"
	"fmt"
	"io/ioutil"
	"strings"
	"strconv"
	common "github.com/matehaxor03/holistic_common/common"
	dao "github.com/matehaxor03/holistic_db_client/dao"
	json "github.com/matehaxor03/holistic_json/json"
	http_extension "github.com/matehaxor03/holistic_http/http_extension"
	helper "github.com/matehaxor03/holistic_db_client/helper"
	validate "github.com/matehaxor03/holistic_db_client/validate"
	monitoring "github.com/matehaxor03/holistic_processor/monitoring"
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
	
	test_read_client, test_read_client_errors := client_manager.GetClient("127.0.0.1", "3306", "holistic", "holistic_r")
	if test_read_client_errors != nil {
		return nil, test_read_client_errors
	}
	
	test_read_database := test_read_client.GetDatabase()
	
	processors := make(map[string](*ProcessorManager))
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
		create_processor, create_processor_errors := NewProcessorManager(client_manager, *domain_name, queue_port, "CreateRecords_" + table_name, 1, -1)
		if create_processor_errors != nil {
			errors = append(errors, create_processor_errors...)
		} else if create_processor != nil {
			processors["CreateRecords_" + table_name] = create_processor
		}

		create_record_processor, create_record_processor_errors := NewProcessorManager(client_manager, *domain_name, queue_port, "CreateRecord_" + table_name, 1, -1)
		if create_record_processor_errors != nil {
			errors = append(errors, create_record_processor_errors...)
		} else if create_record_processor != nil {
			processors["CreateRecord_" + table_name] = create_record_processor
		}

		read_processor, read_processor_errors := NewProcessorManager(client_manager, *domain_name, queue_port, "ReadRecords_" + table_name, 1, -1)
		if read_processor_errors != nil {
			errors = append(errors, read_processor_errors...)
		} else if read_processor != nil {
			processors["ReadRecords_" + table_name] = read_processor
		}

		update_processor, update_processor_errors := NewProcessorManager(client_manager, *domain_name, queue_port, "UpdateRecords_" + table_name, 1, 1)
		if update_processor_errors != nil {
			errors = append(errors, update_processor_errors...)
		} else if update_processor != nil {
			processors["UpdateRecords_" + table_name] = update_processor
		}

		update_record_processor, update_record_processor_errors := NewProcessorManager(client_manager, *domain_name, queue_port, "UpdateRecord_" + table_name, 1, 1)
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

		get_schema_processor, get_schema_processor_errors := NewProcessorManager(client_manager, *domain_name, queue_port, "GetSchema_" + table_name, 1, -1)
		if get_schema_processor_errors != nil {
			errors = append(errors, get_schema_processor_errors...)
		} else if get_schema_processor != nil {
			processors["GetSchema_" + table_name] = get_schema_processor
		}
	}

	commands_list := [...]string{"Run_Sync:1:1",
								"Run_StartBuildBranchInstance:1:1", 
								"Run_NotStarted:1:1", 
								"Run_Start:1:1",
								"Run_CreateSourceFolder:1:1",
								"Run_CreateDomainNameFolder:1:-1",
								"Run_CreateRepositoryAccountFolder:1:1",
								"Run_CreateRepositoryFolder:1:1",
								"Run_CreateBranchesFolder:1:1",
								"Run_CreateTagsFolder:1:1",
								"Run_CreateBranchOrTagFolder:1:1",
								"Run_CloneBranchOrTagFolder:1:1",
								"Run_PullLatestBranchOrTagFolder:1:1",
								"Run_CreateBranchInstancesFolder:1:1",
								"Run_CreateTagInstancesFolder:1:1",
								"Run_CopyToInstanceFolder:1:1",
								"Run_CreateInstanceFolder:1:1",
								"Run_CreateGroup:1:1",
								"Run_CreateUser:1:1",
								"Run_AssignGroupToUser:1:1",
								"Run_AssignGroupToInstanceFolder:1:1",
								"Run_Clean:1:-1",
								"Run_Lint:1:-1",
								"Run_Build:1:-1",
								"Run_UnitTests:1:-1",
								"Run_IntegrationTests:1:-1",
								"Run_IntegrationTestSuite:1:-1",
								"Run_RemoveGroupFromInstanceFolder:1:1",
								"Run_RemoveGroupFromUser:1:1",
								"Run_DeleteGroup:1:1",
								"Run_DeleteUser:1:1",
								"Run_DeleteInstanceFolder:1:1",
								"Run_End:1:1",
								"GetTableNames:1:1"}


	for _, command := range commands_list {
		parts := strings.Split(command, ":")
		if len(parts) != 3 {
			errors = append(errors, fmt.Errorf("parts is not 3 for processor manager: " + command))
			continue
		}
		
		command_name := parts[0]

		minimum_threads, minimum_threads_error := strconv.ParseInt(string(parts[1]), 10, 64)
		if minimum_threads_error != nil {
			errors = append(errors, minimum_threads_error)
			continue
		}

		maximum_threads, maximum_threads_error := strconv.ParseInt(string(parts[2]), 10, 64)
		if maximum_threads_error != nil {
			errors = append(errors, maximum_threads_error)
			continue
		}

		processor, processor_errors := NewProcessorManager(client_manager, *domain_name, queue_port, command_name, int(minimum_threads), int(maximum_threads))
		if processor_errors != nil {
			errors = append(errors, processor_errors...)
		} else if processor != nil {
			processors[command_name] = processor
		} else {
			errors = append(errors, fmt.Errorf("failed to create processor: %s", command_name))
		}

		cpu_load, cpu_load_errors := monitoring.GetCPULoad()
		if cpu_load_errors != nil {
			fmt.Println(cpu_load_errors)
		} else {
			fmt.Println(cpu_load)
		}

		memory_load, memory_load_errors := monitoring.GetMemoryLoad()
		if memory_load_errors != nil {
			fmt.Println(memory_load_errors)
		} else {
			fmt.Println(memory_load)
		}

		cpu_cores := monitoring.GetCPUVirtualCores()
		fmt.Println(cpu_cores)
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
