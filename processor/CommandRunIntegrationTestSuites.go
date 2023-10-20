package processor

import (
	json "github.com/matehaxor03/holistic_json/json"
	common "github.com/matehaxor03/holistic_common/common"
    "path/filepath"
	"os"
	"fmt"
	"strings"
)

func commandRunIntegrationTestSuite(processor *Processor, request *json.Map, response_queue_result *json.Map) []error {
	verify := processor.GetValidator()
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

	parameters_as_map, parameters_as_map_errors := json.Parse(*parameters)
	if parameters_as_map_errors != nil {
		errors = append(errors, parameters_as_map_errors...)
	} else if common.IsNil(parameters_as_map) {
		errors = append(errors, fmt.Errorf("parameters as map is nil"))
	}

	if len(errors) > 0 {
		trigger_next_run_command_errors := triggerNextRunCommand(processor, command_name, branch_instance_step_id, branch_instance_id, branch_id, build_step_id, order, domain_name, repository_account_name, repository_name, branch_name, parameters, created_date, errors, request)
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
		trigger_next_run_command_errors := triggerNextRunCommand(processor, command_name, branch_instance_step_id, branch_instance_id, branch_id, build_step_id, order, domain_name, repository_account_name, repository_name, branch_name, parameters, created_date, errors, request)
		if trigger_next_run_command_errors != nil {
			errors = append(errors, trigger_next_run_command_errors...)
		}
		return errors
	}

	std_callback := getStdoutCallbackFunctionBranch(processor, *command_name, *branch_instance_id, *branch_instance_step_id, *branch_id, *test_suite_name)
	stderr_callback := getStderrCallbackFunctionBranch(processor, *command_name, *branch_instance_id, *branch_instance_step_id, *branch_id, *test_suite_name)

	instance_folder_parts := common.GetDataDirectory()
	instance_folder_parts = append(instance_folder_parts, "src")
	instance_folder_parts = append(instance_folder_parts, *domain_name)
	instance_folder_parts = append(instance_folder_parts, *repository_account_name)
	instance_folder_parts = append(instance_folder_parts, *repository_name)
	instance_folder_parts = append(instance_folder_parts, "branch_instances")
	instance_folder_parts = append(instance_folder_parts, fmt.Sprintf("%d", *branch_instance_id))
	instance_folder_parts = append(instance_folder_parts, *repository_name)
	full_path_of_instance_directory := "/" + filepath.Join(instance_folder_parts...)

	full_path_of_test_suite := full_path_of_instance_directory + *test_suite_name

	if strings.HasSuffix(full_path_of_test_suite, "Integration_test.go") {
		parts := strings.Split(full_path_of_test_suite, "/")
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
	} else {
		errors = append(errors, fmt.Errorf("integration test file does not end with Integration_test.go"))
	}

	if _, stat_error := os.Stat(full_path_of_test_suite); !os.IsNotExist(stat_error) {
		fmt.Println("running " + *test_suite_name)
		bashCommand := common.NewBashCommand()
		command := fmt.Sprintf("cd %s && go test -timeout 7200s -outputdir= -json .%s", full_path_of_instance_directory, *test_suite_name)
		stdout_lines, stderr_lines := bashCommand.ExecuteUnsafeCommandUsingFilesWithoutInputFile(command)
		if stderr_lines != nil {
			errors = append(errors, stderr_lines...)
			fmt.Println("running " + *test_suite_name + " fail")
			for _, stderr_line := range errors {
				(*stderr_callback)(stderr_line)
			}
		} else {
			fmt.Println("running " + *test_suite_name + " pass")
		} 
		
		for _, stdout_line := range stdout_lines {
			(*std_callback)(stdout_line)
		}
	} else {
		errors = append(errors, fmt.Errorf("not found file " + *test_suite_name))
	}

	trigger_next_run_command_errors := triggerNextRunCommand(processor, command_name, branch_instance_step_id, branch_instance_id, branch_id, build_step_id, order, domain_name, repository_account_name, repository_name, branch_name, parameters, created_date, errors, request)
	if trigger_next_run_command_errors != nil {
		return trigger_next_run_command_errors
	}

	return nil
}

func commandRunIntegrationTestSuiteFunc() *func(processor *Processor, request *json.Map, response_queue_result *json.Map) []error {
	funcValue := commandRunIntegrationTestSuite
	return &funcValue
}