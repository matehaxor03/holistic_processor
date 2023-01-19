package processor

import (
	json "github.com/matehaxor03/holistic_json/json"
	common "github.com/matehaxor03/holistic_common/common"
    "path/filepath"
	"os"
	"fmt"
	"strings"
)

func commandRunIntegrationTests(processor *Processor, request *json.Map, response_queue_result *json.Map) []error {
	verify := processor.GetValidator()
	command_name, build_branch_id, build_branch_instance_step_id, build_branch_instance_id, build_step_id, order, domain_name, repository_account_name, repository_name, branch_name, parameters, errors := validateRunCommandHeaders(processor, request)
	if errors == nil {
		var new_errors []error
		errors = new_errors
	} else if len(errors) > 0 {
		trigger_next_run_command_errors := triggerNextRunCommand(processor, command_name, build_branch_id, build_branch_instance_step_id, build_branch_instance_id, build_step_id, order, domain_name, repository_account_name,repository_name, branch_name, parameters, errors, request)
		if trigger_next_run_command_errors != nil {
			errors = append(errors, trigger_next_run_command_errors...)
		}
		return errors
	}

	instance_folder_parts := common.GetDataDirectory()
	instance_folder_parts = append(instance_folder_parts, "src")
	instance_folder_parts = append(instance_folder_parts, *domain_name)
	instance_folder_parts = append(instance_folder_parts, *repository_account_name)
	instance_folder_parts = append(instance_folder_parts, *repository_name)
	instance_folder_parts = append(instance_folder_parts, "branch_instances")
	instance_folder_parts = append(instance_folder_parts, fmt.Sprintf("%d", *build_branch_instance_id))
	instance_folder_parts = append(instance_folder_parts, *repository_name)
	full_path_of_instance_directory := "/" + filepath.Join(instance_folder_parts...)
	//full_path_of_integration_tests_folder := full_path_of_instance_directory + "/tests/integration"
	check_files := make([]string, 0)

	file_error := filepath.Walk(full_path_of_instance_directory,
	func(file_path string, info os.FileInfo, file_error error) error {
		if file_error != nil {
			return file_error
		}
		check_files = append(check_files, file_path)
		return nil
	})
	
	if file_error != nil {
		errors = append(errors, file_error)
	}

	if len(errors) > 0 {
		trigger_next_run_command_errors := triggerNextRunCommand(processor, command_name, build_branch_id, build_branch_instance_step_id, build_branch_instance_id, build_step_id, order, domain_name, repository_account_name, repository_name, branch_name, parameters, errors, request)
		if trigger_next_run_command_errors != nil {
			errors = append(errors, trigger_next_run_command_errors...)
		}
		return errors
	}

	var suite_names []string
	if _, stat_error := os.Stat(full_path_of_instance_directory); !os.IsNotExist(stat_error) {
		for _, check_file := range check_files {
			if strings.HasSuffix(check_file, "Integration_test.go") {
				parts := strings.Split(check_file, "/")
				var part_errors []error
				for index, part := range parts {
					if index == 0 && part == "" {
						continue
					}

					if index == len(parts) - 1 {
						file_name_errors := verify.ValidateFileName(part) 
						if file_name_errors != nil {
							part_errors = append(part_errors, file_name_errors...)
						} 
					} else {
						directory_name_errors := verify.ValidateDirectoryName(part) 
						if directory_name_errors != nil {
							part_errors = append(part_errors, directory_name_errors...)
						}
					}
				}

				if len(part_errors) == 0 {
					suite_names = append(suite_names, strings.TrimPrefix(check_file, full_path_of_instance_directory))
				} else {
					errors = append(errors, part_errors...)
				}
			}
		}

		if len(suite_names) == 0 {
			trigger_next_run_command_errors := triggerNextRunCommand(processor, command_name, build_branch_id, build_branch_instance_step_id, build_branch_instance_id, build_step_id, order, domain_name, repository_account_name, repository_name, branch_name, parameters, errors, request)
			if trigger_next_run_command_errors != nil {
				errors = append(errors, trigger_next_run_command_errors...)
			}

			if len(errors) > 0 {
				return errors
			} else {
				return nil
			}
		}

		read_records_build_step_request_select_fields := []string{"build_step_id", "order"}
		read_records_build_step_request_select_fields_array := json.NewArrayOfValues(common.MapPointerToStringArrayValueToInterface(&read_records_build_step_request_select_fields))
		read_records_build_step_request_where_fields := map[string]interface{}{"name":"Run_IntegrationTestSuite"}
		read_records_build_step_request_where_fields_map := json.NewMapOfValues(&read_records_build_step_request_where_fields)
		read_records_build_step_request_map := map[string]interface{}{"[queue]":"ReadRecords_BuildStep", "[trace_id]":processor.GenerateTraceId(), "[limit]":1}
		read_records_build_step_request := json.NewMapOfValues(&read_records_build_step_request_map)
		read_records_build_step_request.SetMap("[where_fields]", read_records_build_step_request_where_fields_map)
		read_records_build_step_request.SetArray("[select_fields]", read_records_build_step_request_select_fields_array)

		read_records_build_step_response, read_records_build_step_response_errors := processor.SendMessageToQueue(read_records_build_step_request)
		if read_records_build_step_response_errors != nil {
			errors = append(errors, read_records_build_step_response_errors...)
		} else if common.IsNil(read_records_build_step_response) {
			errors = append(errors, fmt.Errorf("read_records_build_branch_instance_step_response is nil"))
		}

		if len(errors) > 0 {
			trigger_next_run_command_errors := triggerNextRunCommand(processor, command_name, build_branch_id, build_branch_instance_step_id, build_branch_instance_id, build_step_id, order, domain_name, repository_account_name, repository_name, branch_name, parameters, errors, request)
			if trigger_next_run_command_errors != nil {
				errors = append(errors, trigger_next_run_command_errors...)
			}
			return errors
		}

		lookup_build_step_array, lookup_build_step_array_errors := read_records_build_step_response.GetArray("data")
		if lookup_build_step_array_errors != nil {
			errors = append(errors, lookup_build_step_array_errors...)
		} else if common.IsNil(lookup_build_step_array) {
			errors = append(errors, fmt.Errorf("lookup_build_step_array is nil"))
		} else if len(*(lookup_build_step_array.GetValues())) != 1 {
			errors = append(errors, fmt.Errorf("lookup_build_step_array does not have one element"))
		}

		if len(errors) > 0 {
			trigger_next_run_command_errors := triggerNextRunCommand(processor, command_name, build_branch_id, build_branch_instance_step_id, build_branch_instance_id, build_step_id, order, domain_name, repository_account_name, repository_name, branch_name, parameters, errors, request)
			if trigger_next_run_command_errors != nil {
				errors = append(errors, trigger_next_run_command_errors...)
			}
			return errors
		}
		
		build_step, build_step_errors := (*(lookup_build_step_array.GetValues()))[0].GetMap()
		if build_step_errors != nil {
			errors = append(errors, build_step_errors...)
		} else if common.IsNil(build_step) {
			errors = append(errors, fmt.Errorf("build_step is nil"))
		}
		
		if len(errors) > 0 {
			trigger_next_run_command_errors := triggerNextRunCommand(processor, command_name, build_branch_id, build_branch_instance_step_id, build_branch_instance_id, build_step_id, order, domain_name, repository_account_name, repository_name, branch_name, parameters, errors, request)
			if trigger_next_run_command_errors != nil {
				errors = append(errors, trigger_next_run_command_errors...)
			}
			return errors
		}

		run_integration_test_suite_build_step_id, run_integration_test_suite_build_step_id_errors := build_step.GetUInt64("build_step_id")
		if run_integration_test_suite_build_step_id_errors != nil {
			errors = append(errors, run_integration_test_suite_build_step_id_errors...)
		} else if common.IsNil(run_integration_test_suite_build_step_id) {
			errors = append(errors, fmt.Errorf("build_step_id is nil"))
		}

		run_integration_test_suite_build_step_id_order, run_integration_test_suite_build_step_id_order_errors := build_step.GetInt64("order")
		if run_integration_test_suite_build_step_id_order_errors != nil {
			errors = append(errors, run_integration_test_suite_build_step_id_order_errors...)
		} else if common.IsNil(run_integration_test_suite_build_step_id_order) {
			errors = append(errors, fmt.Errorf("order is nil"))
		}

		if len(errors) > 0 {
			trigger_next_run_command_errors := triggerNextRunCommand(processor, command_name, build_branch_id, build_branch_instance_step_id, build_branch_instance_id, build_step_id, order, domain_name, repository_account_name, repository_name, branch_name, parameters, errors, request)
			if trigger_next_run_command_errors != nil {
				errors = append(errors, trigger_next_run_command_errors...)
			}
			return errors
		}

		///
		read_records_build_step_sync_request_select_fields := []string{"build_step_id"}
		read_records_build_step_sync_request_select_fields_array := json.NewArrayOfValues(common.MapPointerToStringArrayValueToInterface(&read_records_build_step_sync_request_select_fields))
		read_records_build_step_sync_request_where_fields := map[string]interface{}{"name":"Run_Sync"}
		read_records_build_step_sync_request_where_fields_map := json.NewMapOfValues(&read_records_build_step_sync_request_where_fields)
		read_records_build_step_sync_request_map := map[string]interface{}{"[queue]":"ReadRecords_BuildStep", "[trace_id]":processor.GenerateTraceId(), "[limit]":1}
		read_records_build_step_sync_request := json.NewMapOfValues(&read_records_build_step_sync_request_map)
		read_records_build_step_sync_request.SetMap("[where_fields]", read_records_build_step_sync_request_where_fields_map)
		read_records_build_step_sync_request.SetArray("[select_fields]", read_records_build_step_sync_request_select_fields_array)

		read_records_build_step_sync_response, read_records_build_step_sync_response_errors := processor.SendMessageToQueue(read_records_build_step_sync_request)
		if read_records_build_step_sync_response_errors != nil {
			errors = append(errors, read_records_build_step_sync_response_errors...)
		} else if common.IsNil(read_records_build_step_sync_response) {
			errors = append(errors, fmt.Errorf("read_records_build_step_sync_response is nil"))
		}

		if len(errors) > 0 {
			trigger_next_run_command_errors := triggerNextRunCommand(processor, command_name, build_branch_id, build_branch_instance_step_id, build_branch_instance_id, build_step_id, order, domain_name, repository_account_name, repository_name, branch_name, parameters, errors, request)
			if trigger_next_run_command_errors != nil {
				errors = append(errors, trigger_next_run_command_errors...)
			}
			return errors
		}

		lookup_build_step_sync_array, lookup_build_step_sync_array_errors := read_records_build_step_sync_response.GetArray("data")
		if lookup_build_step_sync_array_errors != nil {
			errors = append(errors, lookup_build_step_sync_array_errors...)
		} else if common.IsNil(lookup_build_step_sync_array) {
			errors = append(errors, fmt.Errorf("lookup_build_step_sync_array is nil"))
		} else if len(*(lookup_build_step_sync_array.GetValues())) != 1 {
			errors = append(errors, fmt.Errorf("lookup_build_step_sync_array does not have one element"))
		}

		if len(errors) > 0 {
			trigger_next_run_command_errors := triggerNextRunCommand(processor, command_name, build_branch_id, build_branch_instance_step_id, build_branch_instance_id, build_step_id, order, domain_name, repository_account_name, repository_name, branch_name, parameters, errors, request)
			if trigger_next_run_command_errors != nil {
				errors = append(errors, trigger_next_run_command_errors...)
			}
			return errors
		}
		
		sync_build_step, sync_build_step_errors := (*(lookup_build_step_sync_array.GetValues()))[0].GetMap()
		if sync_build_step_errors != nil {
			errors = append(errors, sync_build_step_errors...)
		} else if common.IsNil(build_step) {
			errors = append(errors, fmt.Errorf("sync_build_step is nil"))
		}
		
		if len(errors) > 0 {
			trigger_next_run_command_errors := triggerNextRunCommand(processor, command_name, build_branch_id, build_branch_instance_step_id, build_branch_instance_id, build_step_id, order, domain_name, repository_account_name, repository_name, branch_name, parameters, errors, request)
			if trigger_next_run_command_errors != nil {
				errors = append(errors, trigger_next_run_command_errors...)
			}
			return errors
		}

		sync_build_step_id, sync_build_step_id_errors := sync_build_step.GetUInt64("build_step_id")
		if sync_build_step_id_errors != nil {
			errors = append(errors, sync_build_step_id_errors...)
		} else if common.IsNil(sync_build_step_id) {
			errors = append(errors, fmt.Errorf("sync_build_step_id is nil"))
		}

		if len(errors) > 0 {
			trigger_next_run_command_errors := triggerNextRunCommand(processor, command_name, build_branch_id, build_branch_instance_step_id, build_branch_instance_id, build_step_id, order, domain_name, repository_account_name, repository_name, branch_name, parameters, errors, request)
			if trigger_next_run_command_errors != nil {
				errors = append(errors, trigger_next_run_command_errors...)
			}
			return errors
		}

		build_branch_instance_steps := json.NewArray()
		for _, suite_name := range suite_names {
			paramters_map := json.NewMap()
			paramters_map.SetStringValue("test_suite_name", suite_name)

			var parameters_builder strings.Builder
			parameters_json_string_errors := paramters_map.ToJSONString(&parameters_builder)
			
			if parameters_json_string_errors != nil {
				errors = append(errors, parameters_json_string_errors...)
				continue
			}

			build_branch_instance_step := map[string]interface{}{"build_branch_instance_id":*build_branch_instance_id, "build_step_id":*run_integration_test_suite_build_step_id, "order":*run_integration_test_suite_build_step_id_order, "parameters":parameters_builder.String()}
			build_branch_instance_steps.AppendMap(json.NewMapOfValues(&build_branch_instance_step))
		}

		if len(errors) > 0 {
			trigger_next_run_command_errors := triggerNextRunCommand(processor, command_name, build_branch_id, build_branch_instance_step_id, build_branch_instance_id, build_step_id, order, domain_name, repository_account_name, repository_name, branch_name, parameters, errors, request)
			if trigger_next_run_command_errors != nil {
				errors = append(errors, trigger_next_run_command_errors...)
			}
			return errors
		}

		if build_branch_instance_steps.Len() > 0 {
			build_branch_instance_step := map[string]interface{}{"build_branch_instance_id":*build_branch_instance_id, "build_step_id":*sync_build_step_id, "order":(*run_integration_test_suite_build_step_id_order+1)}
			build_branch_instance_steps.AppendMap(json.NewMapOfValues(&build_branch_instance_step))


			create_instance_steps_request := map[string]interface{}{"[queue]":"CreateRecords_BuildBranchInstanceStep", "[trace_id]":processor.GenerateTraceId(),"data":build_branch_instance_steps, "[async]":false}
			create_instance_steps_response, create_instance_steps_response_errors := processor.SendMessageToQueue(json.NewMapOfValues(&create_instance_steps_request))
			if create_instance_steps_response_errors != nil {
				errors = append(errors, create_instance_steps_response_errors...)
			} else if common.IsNil(create_instance_steps_response) {
				errors = append(errors, fmt.Errorf("create_instance_steps_response is nil"))
			}
		}
		
	} else {
		errors = append(errors, fmt.Errorf("not found " + full_path_of_instance_directory))
	}
	
	trigger_next_run_command_errors := triggerNextRunCommand(processor, command_name, build_branch_id, build_branch_instance_step_id, build_branch_instance_id, build_step_id, order, domain_name, repository_account_name, repository_name, branch_name, parameters, errors, request)
	if trigger_next_run_command_errors != nil {
		return trigger_next_run_command_errors
	}
	
	return nil
}

func commandRunIntegrationTestsFunc() *func(processor *Processor, request *json.Map, response_queue_result *json.Map) []error {
	funcValue := commandRunIntegrationTests
	return &funcValue
}