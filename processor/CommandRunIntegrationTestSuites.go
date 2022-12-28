package processor

import (
	json "github.com/matehaxor03/holistic_json/json"
	common "github.com/matehaxor03/holistic_common/common"
    "path/filepath"
	"os"
	"fmt"
)

func commandRunIntegrationTestSuite(processor *Processor, request *json.Map, response_queue_result *json.Map) []error {
	command_name, build_branch_id, build_branch_instance_step_id, build_branch_instance_id, build_step_id, order, domain_name, repository_account_name, repository_name, branch_name, parameters, errors := validateRunCommandHeaders(request)
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

	parameters_as_map, parameters_as_map_errors := json.ParseJSON(*parameters)
	if parameters_as_map_errors != nil {
		errors = append(errors, parameters_as_map_errors...)
	} else if common.IsNil(parameters_as_map) {
		errors = append(errors, fmt.Errorf("parameters as map is nil"))
	}

	if len(errors) > 0 {
		trigger_next_run_command_errors := triggerNextRunCommand(processor, command_name, build_branch_id, build_branch_instance_step_id, build_branch_instance_id, build_step_id, order, domain_name, repository_account_name,repository_name, branch_name, parameters, errors, request)
		if trigger_next_run_command_errors != nil {
			errors = append(errors, trigger_next_run_command_errors...)
		}
		return errors
	}

	test_suite_name, test_suite_name_errors := parameters_as_map.GetString("test_suite_name")
	if test_suite_name_errors != nil {
		errors = append(errors, test_suite_name_errors...)
	} else if common.IsNil(test_suite_name) {
		errors = append(errors, fmt.Errorf("test_suite_name is nil"))
	}

	if len(errors) > 0 {
		trigger_next_run_command_errors := triggerNextRunCommand(processor, command_name, build_branch_id, build_branch_instance_step_id, build_branch_instance_id, build_step_id, order, domain_name, repository_account_name,repository_name, branch_name, parameters, errors, request)
		if trigger_next_run_command_errors != nil {
			errors = append(errors, trigger_next_run_command_errors...)
		}
		return errors
	}

	/*
	 `build_branch_instance_step_id` BIGINT UNSIGNED NOT NULL comment '{"foreign_key":{"table_name":"BuildBranchInstanceStep","column_name":"build_branch_instance_step_id","type":"uint64"}}',
    `log` VARCHAR(1024) NOT NULL DEFAULT '',
    `stdout` BOOLEAN DEFAULT 1,*/

	std_callback := getStdoutCallbackFunctionBranch(processor, *command_name, *build_branch_id, *build_branch_instance_step_id, *test_suite_name)
	stderr_callback := getStderrCallbackFunctionBranch(processor, *command_name, *build_branch_id, *build_branch_instance_step_id, *test_suite_name)

	instance_folder_parts := common.GetDataDirectory()
	instance_folder_parts = append(instance_folder_parts, "src")
	instance_folder_parts = append(instance_folder_parts, *domain_name)
	instance_folder_parts = append(instance_folder_parts, *repository_account_name)
	instance_folder_parts = append(instance_folder_parts, *repository_name)
	instance_folder_parts = append(instance_folder_parts, "branch_instances")
	instance_folder_parts = append(instance_folder_parts, fmt.Sprintf("%d", *build_branch_instance_id))
	instance_folder_parts = append(instance_folder_parts, *repository_name)
	full_path_of_instance_directory := "/" + filepath.Join(instance_folder_parts...)

	test_suite_parts := []string{}
	test_suite_parts = append(test_suite_parts, "tests")
	test_suite_parts = append(test_suite_parts, "integration")
	test_suite_parts = append(test_suite_parts, *test_suite_name)
	test_suite_relative_path := "/" + filepath.Join(test_suite_parts...)

	full_path_of_test_suite := full_path_of_instance_directory + test_suite_relative_path

	if _, stat_error := os.Stat(full_path_of_test_suite); !os.IsNotExist(stat_error) {
		fmt.Println("running " + *test_suite_name)
		bashCommand := common.NewBashCommand()
		command := fmt.Sprintf("cd %s && go test -outputdir= -json .%s", full_path_of_instance_directory, test_suite_relative_path)
		_, bash_command_errors := bashCommand.ExecuteUnsafeCommand(command, std_callback, stderr_callback)
		if bash_command_errors != nil {
			errors = append(errors, bash_command_errors...)
		} 
	} else {
		fmt.Println("not found file " + *test_suite_name)
	}

	trigger_next_run_command_errors := triggerNextRunCommand(processor, command_name, build_branch_id, build_branch_instance_step_id, build_branch_instance_id, build_step_id, order, domain_name, repository_account_name,repository_name, branch_name, parameters, errors, request)
	if trigger_next_run_command_errors != nil {
		return trigger_next_run_command_errors
	}

	return nil
}

func commandRunIntegrationTestSuiteFunc() *func(processor *Processor, request *json.Map, response_queue_result *json.Map) []error {
	funcValue := commandRunIntegrationTestSuite
	return &funcValue
}