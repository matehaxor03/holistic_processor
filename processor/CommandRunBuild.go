package processor

import (
	json "github.com/matehaxor03/holistic_json/json"
	common "github.com/matehaxor03/holistic_common/common"
    "path/filepath"
	"fmt"
	"os"
)

func commandRunBuild(processor *Processor, request *json.Map, response_queue_result *json.Map) []error {
	command_name, branch_instance_step_id, branch_instance_id, branch_id, build_step_id, order, domain_name, repository_account_name, repository_name, branch_name, parameters, created_date, errors := validateRunCommandHeaders(processor, request)
	if errors == nil {
		var new_errors []error
		errors = new_errors
	} else if len(errors) > 0 {
		trigger_next_run_command_errors := triggerNextRunCommand(processor, command_name, branch_instance_step_id, branch_instance_id, branch_id, build_step_id, order, domain_name, repository_account_name, repository_name, branch_name, parameters, created_date, errors, request)
		if trigger_next_run_command_errors != nil {
			errors = append(errors, trigger_next_run_command_errors...)
		}
		return errors
	}

	host_user := processor.GetHostUser()
	home_directory, home_directory_errors := host_user.GetHomeDirectoryAbsoluteDirectory()
	if home_directory_errors != nil {
		errors = append(errors, home_directory_errors...)
	}

	if len(errors) > 0 {
		trigger_next_run_command_errors := triggerNextRunCommand(processor, command_name, branch_instance_step_id, branch_instance_id, branch_id, build_step_id, order, domain_name, repository_account_name, repository_name, branch_name, parameters, created_date, errors, request)
		if trigger_next_run_command_errors != nil {
			errors = append(errors, trigger_next_run_command_errors...)
		}
		return errors
	}

	instance_folder_parts := home_directory.GetPath()
	instance_folder_parts = append(instance_folder_parts, "src")
	instance_folder_parts = append(instance_folder_parts, *domain_name)
	instance_folder_parts = append(instance_folder_parts, *repository_account_name)
	instance_folder_parts = append(instance_folder_parts, *repository_name)
	instance_folder_parts = append(instance_folder_parts, "branch_instances")
	instance_folder_parts = append(instance_folder_parts, fmt.Sprintf("%d", *branch_instance_id))
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
		_, build_bash_command_errors := bashCommand.ExecuteUnsafeCommandUsingFilesWithoutInputFile(build_command)
		if build_bash_command_errors != nil {
			errors = append(errors, build_bash_command_errors...)
		} 
	} 

	if _, stat_error := os.Stat(full_path_of_integration_tests_folder); !os.IsNotExist(stat_error) {
		build_tests_command := fmt.Sprintf("cd %s && go clean -testcache | go test -c -outputdir= .%s", full_path_of_instance_directory, test_integration_relative_path)
		_, build_tests_bash_command_errors := bashCommand.ExecuteUnsafeCommandUsingFilesWithoutInputFile(build_tests_command)
		if build_tests_bash_command_errors != nil {
			errors = append(errors, build_tests_bash_command_errors...)
		} 
	} 

	trigger_next_run_command_errors := triggerNextRunCommand(processor, command_name, branch_instance_step_id, branch_instance_id, branch_id, build_step_id, order, domain_name, repository_account_name, repository_name, branch_name, parameters, created_date, errors, request)
	if trigger_next_run_command_errors != nil {
		return trigger_next_run_command_errors
	}

	return nil
}

func commandRunBuildFunc() *func(processor *Processor, request *json.Map, response_queue_result *json.Map) []error {
	funcValue := commandRunBuild
	return &funcValue
}