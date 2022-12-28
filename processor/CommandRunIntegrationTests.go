package processor

import (
	json "github.com/matehaxor03/holistic_json/json"
	common "github.com/matehaxor03/holistic_common/common"
	"io/ioutil"
    "path/filepath"
	"os"
	"fmt"
	"strings"
)

func commandRunIntegrationTests(processor *Processor, request *json.Map, response_queue_result *json.Map) []error {
	command_name, build_branch_id, build_branch_instance_step_id, build_branch_instance_id, build_step_id, order, domain_name, repository_account_name, repository_name, branch_name, parameters, errors := validateRunCommandHeaders(request)
	if errors != nil {
		return errors
	} else {
		var new_errors []error
		errors = new_errors
	}


	/*
	 `build_branch_instance_step_id` BIGINT UNSIGNED NOT NULL comment '{"foreign_key":{"table_name":"BuildBranchInstanceStep","column_name":"build_branch_instance_step_id","type":"uint64"}}',
    `log` VARCHAR(1024) NOT NULL DEFAULT '',
    `stdout` BOOLEAN DEFAULT 1,*/

	//std_callback := getStdoutCallbackFunctionBranch(processor, *command_name, *build_branch_id, *build_branch_instance_step_id)
	//stderr_callback := getStderrCallbackFunctionBranch(processor, *command_name, *build_branch_id, *build_branch_instance_step_id)

	instance_folder_parts := common.GetDataDirectory()
	instance_folder_parts = append(instance_folder_parts, "src")
	instance_folder_parts = append(instance_folder_parts, *domain_name)
	instance_folder_parts = append(instance_folder_parts, *repository_account_name)
	instance_folder_parts = append(instance_folder_parts, *repository_name)
	instance_folder_parts = append(instance_folder_parts, "branch_instances")
	instance_folder_parts = append(instance_folder_parts, fmt.Sprintf("%d", *build_branch_instance_id))
	instance_folder_parts = append(instance_folder_parts, *repository_name)
	full_path_of_instance_directory := "/" + filepath.Join(instance_folder_parts...)
	full_path_of_integration_tests_folder := full_path_of_instance_directory + "/tests/integration"


	if _, stat_error := os.Stat(full_path_of_integration_tests_folder); !os.IsNotExist(stat_error) {
		files, files_error := ioutil.ReadDir(full_path_of_integration_tests_folder)
		if files_error != nil {
			errors = append(errors, files_error)
		}

		if len(errors) > 0 {
			return errors
		}

		var suite_names []string
		for _, file := range files {
			filename := file.Name()
			if strings.HasSuffix(filename, "test.go") {
				suite_names = append(suite_names, filename)
			}
		}

		if len(suite_names) > 0 {
			read_records_build_step_request := json.Map{"[queue]":"ReadRecords_BuildStep", "[trace_id]":processor.GenerateTraceId(), "[where_fields]":json.Map{"name":"Run_IntegrationTestSuite"}, "[select_fields]": json.Array{"build_step_id", "order"}, "[limit]":1}
			read_records_build_step_response, read_records_build_step_response_errors := processor.SendMessageToQueue(&read_records_build_step_request)
			if read_records_build_step_response_errors != nil {
				errors = append(errors, read_records_build_step_response_errors...)
			} else if common.IsNil(read_records_build_step_response) {
				errors = append(errors, fmt.Errorf("read_records_build_branch_instance_step_response is nil"))
			}

			if len(errors) > 0 {
				return errors
			}

			lookup_build_step_array, lookup_build_step_array_errors := read_records_build_step_response.GetArray("data")
			if lookup_build_step_array_errors != nil {
				errors = append(errors, lookup_build_step_array_errors...)
			} else if common.IsNil(lookup_build_step_array) {
				errors = append(errors, fmt.Errorf("lookup_build_step_array is nil"))
			} else if len(*lookup_build_step_array) != 1 {
				errors = append(errors, fmt.Errorf("lookup_build_step_array does not have one element"))
			}

			if len(errors) > 0 {
				return errors
			}
		
			var build_step json.Map
			build_step_interface := (*lookup_build_step_array)[0]
			type_of_build_step := common.GetType(build_step_interface)
		
			if type_of_build_step == "json.Map" {
				build_step = build_step_interface.(json.Map)
			} else if type_of_build_step == "*json.Map" {
				build_step = *(build_step_interface.(*json.Map))
			} else {
				errors = append(errors, fmt.Errorf("build step has invalid type"))
			}
		
			if len(errors) > 0 {
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
				return errors
			}

			build_branch_instance_steps := json.Array{}
			for _, suite_name := range suite_names {
				fmt.Println(suite_name)
				paramters_map := json.Map{}
				paramters_map.SetString("test_suite_name", &suite_name)

				var parameters_builder strings.Builder
				parameters_json_string_errors := paramters_map.ToJSONString(&parameters_builder)
				if parameters_json_string_errors != nil {
					errors = append(errors, parameters_json_string_errors...)
					continue
				}

				build_branch_instance_step := json.Map{"build_branch_instance_id":*build_branch_instance_id, "build_step_id":*run_integration_test_suite_build_step_id, "order":*run_integration_test_suite_build_step_id_order, "parameters":parameters_builder.String()}
				build_branch_instance_steps = append(build_branch_instance_steps, build_branch_instance_step)
			}

			if len(errors) > 0 {
				return errors
			}

			create_instance_steps_request := json.Map{"[queue]":"CreateRecords_BuildBranchInstanceStep", "[trace_id]":processor.GenerateTraceId(), "data":build_branch_instance_steps}
			create_instance_steps_response, create_instance_steps_response_errors := processor.SendMessageToQueue(&create_instance_steps_request)
			if create_instance_steps_response_errors != nil {
				errors = append(errors, create_instance_steps_response_errors...)
			} else if common.IsNil(create_instance_steps_response) {
				errors = append(errors, fmt.Errorf("create_instance_steps_response is nil"))
			}

			if len(errors) > 0 {
				return errors
			}
		}
	}

	if len(errors) > 0 {
		return errors
	}

	trigger_next_run_command_errors := triggerNextRunCommand(processor, command_name, build_branch_id, build_branch_instance_step_id, build_branch_instance_id, build_step_id, order, domain_name, repository_account_name, repository_name, branch_name, parameters, request)
	if trigger_next_run_command_errors != nil {
		return trigger_next_run_command_errors
	}

	return nil
}

func commandRunIntegrationTestsFunc() *func(processor *Processor, request *json.Map, response_queue_result *json.Map) []error {
	funcValue := commandRunIntegrationTests
	return &funcValue
}