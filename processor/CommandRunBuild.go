package processor

import (
	json "github.com/matehaxor03/holistic_json/json"
	common "github.com/matehaxor03/holistic_common/common"
    "path/filepath"
	"fmt"
	"os"
)

func commandRunBuild(processor *Processor, request *json.Map, response_queue_result *json.Map) []error {
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

	std_callback := func(message string) {
		fmt.Println(message)
	}

	stderr_callback := func(message error) {
		fmt.Println(message)
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
	
	test_integration_parts := []string{}
	test_integration_parts = append(test_integration_parts, "tests")
	test_integration_parts = append(test_integration_parts, "integration")
	test_integration_relative_path := "/" + filepath.Join(test_integration_parts...)
	full_path_of_integration_tests_folder := full_path_of_instance_directory + test_integration_relative_path

	bashCommand := common.NewBashCommand()

	if _, stat_error := os.Stat(full_path_of_instance_directory); !os.IsNotExist(stat_error) {
		build_command := fmt.Sprintf("cd %s && go build", full_path_of_instance_directory)
		_, build_bash_command_errors := bashCommand.ExecuteUnsafeCommand(build_command, &std_callback, &stderr_callback)
		if build_bash_command_errors != nil {
			errors = append(errors, build_bash_command_errors...)
		} 
	} 

	if _, stat_error := os.Stat(full_path_of_integration_tests_folder); !os.IsNotExist(stat_error) {
		build_tests_command := fmt.Sprintf("cd %s && go clean -testcache | go test -c -outputdir= .%s", full_path_of_instance_directory, test_integration_relative_path)
		_, build_tests_bash_command_errors := bashCommand.ExecuteUnsafeCommand(build_tests_command, &std_callback, &stderr_callback)
		if build_tests_bash_command_errors != nil {
			errors = append(errors, build_tests_bash_command_errors...)
		} 
	} 

	trigger_next_run_command_errors := triggerNextRunCommand(processor, command_name, build_branch_id, build_branch_instance_step_id, build_branch_instance_id, build_step_id, order, domain_name, repository_account_name,repository_name, branch_name, parameters, errors, request)
	if trigger_next_run_command_errors != nil {
		return trigger_next_run_command_errors
	}

	return nil
}

func commandRunBuildFunc() *func(processor *Processor, request *json.Map, response_queue_result *json.Map) []error {
	funcValue := commandRunBuild
	return &funcValue
}