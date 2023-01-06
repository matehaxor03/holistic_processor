package processor

import (
	"net/http"
	"fmt"
	"io/ioutil"
	common "github.com/matehaxor03/holistic_common/common"
	db_client "github.com/matehaxor03/holistic_db_client/db_client"
	dao "github.com/matehaxor03/holistic_db_client/dao"
	json "github.com/matehaxor03/holistic_json/json"
	http_extension "github.com/matehaxor03/holistic_http/http_extension"
	helper "github.com/matehaxor03/holistic_db_client/helper"
)

type ProcessorServer struct {
	Start   func() ([]error)
}

func NewProcessorServer(port string, server_crt_path string, server_key_path string, queue_domain_name string, queue_port string) (*ProcessorServer, []error) {
	var errors []error
	struct_type := "processor.ProcessorServer"
	client_manager, client_manager_errors := db_client.NewClientManager()
	if client_manager_errors != nil {
		return nil, client_manager_errors
	}

	test_read_client, test_read_client_errors := client_manager.GetClient("holistic_db_config#127.0.0.1#3306#holistic#holistic_read")
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

	domain_name, domain_name_errors := dao.NewDomainName(queue_domain_name)
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
		temp_value, temp_value_errors := helper.GetField(struct_type, getData(), "[system_schema]", "[system_fields]", "[port]", "string")
		if temp_value_errors != nil {
			return "",temp_value_errors
		}
		return temp_value.(string), nil
	}

	getServerCrtPath := func() (string, []error) {
		temp_value, temp_value_errors := helper.GetField(struct_type, getData(), "[system_schema]", "[system_fields]", "[server_crt_path]", "string")
		if temp_value_errors != nil {
			return "",temp_value_errors
		}
		return temp_value.(string), nil
	}

	getServerKeyPath := func() (string, []error) {
		temp_value, temp_value_errors := helper.GetField(struct_type, getData(), "[system_schema]", "[system_fields]", "[server_key_path]", "string")
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


	for _, table_name := range *table_names {
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

	run_sync_processor, run_sync_processor_errors := NewProcessor(client_manager, *domain_name, queue_port, "Run_Sync")
	if run_sync_processor_errors != nil {
		errors = append(errors, run_sync_processor_errors...)
	} else if run_sync_processor != nil {
		processors["Run_Sync"] = run_sync_processor
	}

	run_build_branch_instance_processor, run_build_branch_instance_processor_errors := NewProcessor(client_manager, *domain_name, queue_port, "Run_StartBuildBranchInstance")
	if run_build_branch_instance_processor_errors != nil {
		errors = append(errors, run_build_branch_instance_processor_errors...)
	} else if run_build_branch_instance_processor != nil {
		processors["Run_StartBuildBranchInstance"] = run_build_branch_instance_processor
	}

	run_not_started_processor, run_not_started_processor_errors := NewProcessor(client_manager, *domain_name, queue_port, "Run_NotStarted")
	if run_not_started_processor_errors != nil {
		errors = append(errors, run_not_started_processor_errors...)
	} else if run_not_started_processor != nil {
		processors["Run_NotStarted"] = run_not_started_processor
	}

	run_start_processor, run_start_processor_errors := NewProcessor(client_manager, *domain_name, queue_port, "Run_Start")
	if run_start_processor_errors != nil {
		errors = append(errors, run_start_processor_errors...)
	} else if run_not_started_processor != nil {
		processors["Run_Start"] = run_start_processor
	}

	run_create_sources_processor, run_create_sources_processor_errors := NewProcessor(client_manager, *domain_name, queue_port, "Run_CreateSourceFolder")
	if run_create_sources_processor_errors != nil {
		errors = append(errors, run_create_sources_processor_errors...)
	} else if run_not_started_processor != nil {
		processors["Run_CreateSourceFolder"] = run_create_sources_processor
	}

	run_create_domain_name_processor, run_create_domain_name_processor_errors := NewProcessor(client_manager, *domain_name, queue_port, "Run_CreateDomainNameFolder")
	if run_create_domain_name_processor_errors != nil {
		errors = append(errors, run_create_domain_name_processor_errors...)
	} else if run_create_domain_name_processor != nil {
		processors["Run_CreateDomainNameFolder"] = run_create_domain_name_processor
	}

	run_create_repository_account_processor, run_create_repository_account_processor_errors := NewProcessor(client_manager, *domain_name, queue_port, "Run_CreateRepositoryAccountFolder")
	if run_create_repository_account_processor_errors != nil {
		errors = append(errors, run_create_repository_account_processor_errors...)
	} else if run_create_repository_account_processor != nil {
		processors["Run_CreateRepositoryAccountFolder"] = run_create_repository_account_processor
	}

	run_create_repository_processor, run_create_repository_processor_errors := NewProcessor(client_manager, *domain_name, queue_port, "Run_CreateRepositoryFolder")
	if run_create_repository_processor_errors != nil {
		errors = append(errors, run_create_repository_processor_errors...)
	} else if run_create_repository_processor != nil {
		processors["Run_CreateRepositoryFolder"] = run_create_repository_processor
	}

	run_create_branches_processor, run_create_branches_processor_errors := NewProcessor(client_manager, *domain_name, queue_port, "Run_CreateBranchesFolder")
	if run_create_branches_processor_errors != nil {
		errors = append(errors, run_create_branches_processor_errors...)
	} else if run_create_branches_processor != nil {
		processors["Run_CreateBranchesFolder"] = run_create_branches_processor
	}

	run_create_tags_processor, run_create_tags_processor_errors := NewProcessor(client_manager, *domain_name, queue_port, "Run_CreateTagsFolder")
	if run_create_tags_processor_errors != nil {
		errors = append(errors, run_create_tags_processor_errors...)
	} else if run_create_tags_processor != nil {
		processors["Run_CreateTagsFolder"] = run_create_tags_processor
	}

	run_create_branch_or_tag_folder_processor, run_create_branch_or_tag_folder_processor_errors := NewProcessor(client_manager, *domain_name, queue_port, "Run_CreateBranchOrTagFolder")
	if run_create_branch_or_tag_folder_processor_errors != nil {
		errors = append(errors, run_create_branch_or_tag_folder_processor_errors...)
	} else if run_create_branch_or_tag_folder_processor != nil {
		processors["Run_CreateBranchOrTagFolder"] = run_create_branch_or_tag_folder_processor
	}

	run_clone_branch_or_tag_processor, run_clone_branch_or_tag_processor_errors := NewProcessor(client_manager, *domain_name, queue_port, "Run_CloneBranchOrTagFolder")
	if run_clone_branch_or_tag_processor_errors != nil {
		errors = append(errors, run_clone_branch_or_tag_processor_errors...)
	} else if run_clone_branch_or_tag_processor != nil {
		processors["Run_CloneBranchOrTagFolder"] = run_clone_branch_or_tag_processor
	}

	run_pull_latest_branch_or_tag_processor, run_pull_latest_branch_or_tag_processor_errors := NewProcessor(client_manager, *domain_name, queue_port, "Run_PullLatestBranchOrTagFolder")
	if run_pull_latest_branch_or_tag_processor_errors != nil {
		errors = append(errors, run_pull_latest_branch_or_tag_processor_errors...)
	} else if run_pull_latest_branch_or_tag_processor != nil {
		processors["Run_PullLatestBranchOrTagFolder"] = run_pull_latest_branch_or_tag_processor
	}

	run_create_branch_instances_folder_processor, run_create_branch_instances_folder_processor_errors := NewProcessor(client_manager, *domain_name, queue_port, "Run_CreateBranchInstancesFolder")
	if run_create_branch_instances_folder_processor_errors != nil {
		errors = append(errors, run_create_branch_instances_folder_processor_errors...)
	} else if run_create_branch_instances_folder_processor != nil {
		processors["Run_CreateBranchInstancesFolder"] = run_create_branch_instances_folder_processor
	}

	run_create_tag_instances_folder_processor, run_create_tag_instances_folder_processor_errors := NewProcessor(client_manager, *domain_name, queue_port, "Run_CreateTagInstancesFolder")
	if run_create_tag_instances_folder_processor_errors != nil {
		errors = append(errors, run_create_tag_instances_folder_processor_errors...)
	} else if run_create_tag_instances_folder_processor != nil {
		processors["Run_CreateTagInstancesFolder"] = run_create_tag_instances_folder_processor
	}

	run_copy_to_instance_folder_processor, run_copy_to_instance_folder_processor_errors := NewProcessor(client_manager, *domain_name, queue_port, "Run_CopyToInstanceFolder")
	if run_copy_to_instance_folder_processor_errors != nil {
		errors = append(errors, run_copy_to_instance_folder_processor_errors...)
	} else if run_copy_to_instance_folder_processor != nil {
		processors["Run_CopyToInstanceFolder"] = run_copy_to_instance_folder_processor
	}

	run_create_instance_folder_processor, run_create_instance_folder_processor_errors := NewProcessor(client_manager, *domain_name, queue_port, "Run_CreateInstanceFolder")
	if run_create_instance_folder_processor_errors != nil {
		errors = append(errors, run_create_instance_folder_processor_errors...)
	} else if run_create_instance_folder_processor != nil {
		processors["Run_CreateInstanceFolder"] = run_create_instance_folder_processor
	}

	run_create_group_processor, run_create_group_processor_errors := NewProcessor(client_manager, *domain_name, queue_port, "Run_CreateGroup")
	if run_create_group_processor_errors != nil {
		errors = append(errors, run_create_group_processor_errors...)
	} else if run_create_group_processor != nil {
		processors["Run_CreateGroup"] = run_create_group_processor
	}

	run_create_user_processor, run_create_user_processor_errors := NewProcessor(client_manager, *domain_name, queue_port, "Run_CreateUser")
	if run_create_user_processor_errors != nil {
		errors = append(errors, run_create_user_processor_errors...)
	} else if run_create_user_processor != nil {
		processors["Run_CreateUser"] = run_create_user_processor
	}

	run_assign_group_to_user_processor, run_assign_group_to_user_processor_errors := NewProcessor(client_manager, *domain_name, queue_port, "Run_AssignGroupToUser")
	if run_assign_group_to_user_processor_errors != nil {
		errors = append(errors, run_assign_group_to_user_processor_errors...)
	} else if run_assign_group_to_user_processor != nil {
		processors["Run_AssignGroupToUser"] = run_assign_group_to_user_processor
	}

	run_assign_group_to_instance_folder_processor, run_assign_group_to_instance_folder_processor_errors := NewProcessor(client_manager, *domain_name, queue_port, "Run_AssignGroupToInstanceFolder")
	if run_assign_group_to_instance_folder_processor_errors != nil {
		errors = append(errors, run_assign_group_to_instance_folder_processor_errors...)
	} else if run_assign_group_to_instance_folder_processor != nil {
		processors["Run_AssignGroupToInstanceFolder"] = run_assign_group_to_instance_folder_processor
	}

	run_clean_processor, run_clean_processor_errors := NewProcessor(client_manager, *domain_name, queue_port, "Run_Clean")
	if run_clean_processor_errors != nil {
		errors = append(errors, run_clean_processor_errors...)
	} else if run_clean_processor != nil {
		processors["Run_Clean"] = run_clean_processor
	}

	run_lint_processor, run_lint_processor_errors := NewProcessor(client_manager, *domain_name, queue_port, "Run_Lint")
	if run_lint_processor_errors != nil {
		errors = append(errors, run_lint_processor_errors...)
	} else if run_lint_processor != nil {
		processors["Run_Lint"] = run_lint_processor
	}

	run_build_processor, run_build_processor_errors := NewProcessor(client_manager, *domain_name, queue_port, "Run_Build")
	if run_build_processor_errors != nil {
		errors = append(errors, run_build_processor_errors...)
	} else if run_build_processor != nil {
		processors["Run_Build"] = run_build_processor
	}

	run_unit_tests_processor, run_unit_tests_processor_errors := NewProcessor(client_manager, *domain_name, queue_port, "Run_UnitTests")
	if run_unit_tests_processor_errors != nil {
		errors = append(errors, run_unit_tests_processor_errors...)
	} else if run_unit_tests_processor != nil {
		processors["Run_UnitTests"] = run_unit_tests_processor
	}

	run_integration_tests_processor, run_integration_tests_processor_errors := NewProcessor(client_manager, *domain_name, queue_port, "Run_IntegrationTests")
	if run_integration_tests_processor_errors != nil {
		errors = append(errors, run_integration_tests_processor_errors...)
	} else if run_integration_tests_processor != nil {
		processors["Run_IntegrationTests"] = run_integration_tests_processor
	}

	run_integration_test_suite_processor, run_integration_test_suite_processor_errors := NewProcessor(client_manager, *domain_name, queue_port, "Run_IntegrationTestSuite")
	if run_integration_test_suite_processor_errors != nil {
		errors = append(errors, run_integration_test_suite_processor_errors...)
	} else if run_integration_test_suite_processor != nil {
		processors["Run_IntegrationTestSuite"] = run_integration_test_suite_processor
	}

	run_remove_group_from_instance_folder_processor, run_remove_group_from_instance_folder_processor_errors := NewProcessor(client_manager, *domain_name, queue_port, "Run_RemoveGroupFromInstanceFolder")
	if run_remove_group_from_instance_folder_processor_errors != nil {
		errors = append(errors, run_remove_group_from_instance_folder_processor_errors...)
	} else if run_remove_group_from_instance_folder_processor != nil {
		processors["Run_RemoveGroupFromInstanceFolder"] = run_remove_group_from_instance_folder_processor
	}

	run_remove_group_from_user_processor, run_remove_group_from_user_processor_errors := NewProcessor(client_manager, *domain_name, queue_port, "Run_RemoveGroupFromUser")
	if run_remove_group_from_user_processor_errors != nil {
		errors = append(errors, run_remove_group_from_user_processor_errors...)
	} else if run_remove_group_from_user_processor != nil {
		processors["Run_RemoveGroupFromUser"] = run_remove_group_from_user_processor
	}

	run_delete_group_processor, run_delete_group_processor_errors := NewProcessor(client_manager, *domain_name, queue_port, "Run_DeleteGroup")
	if run_delete_group_processor_errors != nil {
		errors = append(errors, run_delete_group_processor_errors...)
	} else if run_delete_group_processor != nil {
		processors["Run_DeleteGroup"] = run_delete_group_processor
	}

	run_delete_user_processor, run_delete_user_processor_errors := NewProcessor(client_manager, *domain_name, queue_port, "Run_DeleteUser")
	if run_delete_user_processor_errors != nil {
		errors = append(errors, run_delete_user_processor_errors...)
	} else if run_delete_user_processor != nil {
		processors["Run_DeleteUser"] = run_delete_user_processor
	}

	run_delete_instance_folder_processor, run_delete_instance_folder_processor_errors := NewProcessor(client_manager, *domain_name, queue_port, "Run_DeleteInstanceFolder")
	if run_delete_instance_folder_processor_errors != nil {
		errors = append(errors, run_delete_instance_folder_processor_errors...)
	} else if run_delete_instance_folder_processor != nil {
		processors["Run_DeleteInstanceFolder"] = run_delete_instance_folder_processor
	}

	run_end_processor, run_end_processor_errors := NewProcessor(client_manager, *domain_name, queue_port, "Run_End")
	if run_end_processor_errors != nil {
		errors = append(errors, run_end_processor_errors...)
	} else if run_end_processor != nil {
		processors["Run_End"] = run_end_processor
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
