package processor

import (
	json "github.com/matehaxor03/holistic_json/json"
	common "github.com/matehaxor03/holistic_common/common"
	"strings"
	"fmt"
)

func getStdoutCallbackFunctionBranch(processor *Processor, command_name string, build_branch_id uint64, build_branch_instance_step_id uint64) (*func(message string)) {
	this_processor := processor
	this_command_name := command_name
	this_build_branch_id := build_branch_id
	this_build_branch_instance_step_id := build_branch_instance_step_id
	
	
	function := func(message string) {
		callback_payload := json.Map{"CreateRecord_BuildBranchInstanceStepLog":json.Map{"data":json.Map{"build_branch_instance_step_id":this_build_branch_instance_step_id,"log":message,"stdout":true},"[queue_mode]":"PushBack","[async]":true, "[trace_id]":this_processor.GenerateTraceId()}}
		processor.SendMessageToQueueFireAndForget(&callback_payload)

		if this_command_name == "Run_IntegrationTests" {
			message = strings.ReplaceAll(message, "\\\"", "\"")
			json_message, json_message_errors := json.ParseJSON(message)
			if json_message_errors == nil && json_message != nil {
				test_suite_value, test_suite_value_errors := json_message.GetString("Package")
				test_value, test_value_errors := json_message.GetString("Test")
				test_result_value, test_result_value_errors := json_message.GetString("Action")
				if test_suite_value_errors != nil &&
					test_suite_value != nil && 
					test_value != nil &&
					test_value_errors != nil &&
					test_result_value != nil &&
					test_result_value_errors != nil &&
					(*test_result_value == "pass" || *test_result_value == "fail") {
					
					
					select_test_suite_payload := json.Map{"ReadRecords_TestSuiteBuildBranch":json.Map{"data":json.Map{"[where_fields]":json.Map{"build_branch_id":this_build_branch_id,"name":*test_suite_value}, "[limit]":1},"[queue_mode]":"PushBack","[async]":false,"[trace_id]":this_processor.GenerateTraceId()}}
					test_suite_response, test_suite_response_errors := processor.SendMessageToQueue(&select_test_suite_payload)
					if test_suite_response_errors != nil {
						return
					}

					if test_suite_response == nil {
						return
					}

					json_inner_test_suite_response, json_inner_test_suite_response_errors := test_suite_response.GetMap(test_suite_response.Keys()[0]) 
					if json_inner_test_suite_response_errors != nil {
						return
					}

					if json_inner_test_suite_response == nil {
						return
					}

					test_suite_response_array, test_suite_response_array_errors := json_inner_test_suite_response.GetArray("data")
					if test_suite_response_array_errors != nil {
						return
					}

					if test_suite_response_array == nil {
						return
					}

					var test_suite_map json.Map
					if len(*test_suite_response_array) == 0 {
						create_test_suite_payload := json.Map{"CreateRecord_TestSuiteBuildBranch":json.Map{"data":json.Map{"build_branch_id":this_build_branch_id,"name":*test_suite_value}},"[queue_mode]":"PushBack","[async]":false, "[trace_id]":this_processor.GenerateTraceId()}
						create_suite_response, create_suite_response_errors := processor.SendMessageToQueue(&create_test_suite_payload)
						if create_suite_response_errors != nil {
							return
						}

						if create_suite_response == nil {
							return
						}

						json_inner_create_suite_response, json_inner_create_suite_response_errors := create_suite_response.GetMap(create_suite_response.Keys()[0]) 
						if json_inner_create_suite_response_errors != nil {
							return
						}

						if json_inner_create_suite_response == nil {
							return
						}

						created_test_suite_map, created_test_suite_map_errors := json_inner_create_suite_response.GetMap("data")
						if created_test_suite_map_errors != nil {
							return 
						}

						if created_test_suite_map == nil {
							return
						}

						test_suite_map = *created_test_suite_map
					} else if len(*test_suite_response_array) == 1 {
						
						test_suite_interface := (*test_suite_response_array)[0]
						type_of_test_suite_interface := common.GetType(test_suite_interface)

						if type_of_test_suite_interface == "json.Map" {
							test_suite_map = test_suite_interface.(json.Map)
						} else if type_of_test_suite_interface == "*json.Map" {
							test_suite_map = *(test_suite_interface.(*json.Map))
						} else {
							return
						}

					} else {
						return
					}

					test_suite_build_branch_id, test_suite_build_branch_id_errors := test_suite_map.GetUInt64("test_suite_build_branch_id")
					if test_suite_build_branch_id_errors != nil {
						return
					}

					if test_suite_build_branch_id == nil {
						return
					}

					select_test_payload := json.Map{"ReadRecords_TestBuildBranch":json.Map{"data":json.Map{"[where_fields]":json.Map{"test_suite_build_branch_id":*test_suite_build_branch_id,"name":*test_value}, "[limit]":1},"[queue_mode]":"PushBack","[async]":false, "[trace_id]":this_processor.GenerateTraceId()}}
					test_response, test_response_errors := processor.SendMessageToQueue(&select_test_payload)
					if test_response_errors != nil {
						return
					}

					if test_response == nil {
						return
					}

					json_inner_test_response, json_inner_test_response_errors := test_response.GetMap(test_response.Keys()[0]) 
					if json_inner_test_response_errors != nil {
						return
					}

					if json_inner_test_response == nil {
						return
					}

					test_response_array, test_response_array_errors := json_inner_test_response.GetArray("data")
					if test_response_array_errors != nil {
						return
					}

					if test_response_array == nil {
						return
					}

					var test_map json.Map
					if len(*test_response_array) == 0 {
						create_test_payload := json.Map{"CreateRecord_TestBuildBranch":json.Map{"data":json.Map{"test_suite_build_branch_id":test_suite_build_branch_id,"name":*test_value}},"[queue_mode]":"PushBack","[async]":false, "[trace_id]":this_processor.GenerateTraceId()}
						create_test_response, create_test_response_errors := processor.SendMessageToQueue(&create_test_payload)
						if create_test_response_errors != nil {
							return
						}

						if create_test_response == nil {
							return
						}

						json_inner_create_test_response, json_inner_create_test_response_errors := create_test_response.GetMap(create_test_response.Keys()[0]) 
						if json_inner_create_test_response_errors != nil {
							return
						}

						if json_inner_create_test_response == nil {
							return
						}

						created_test_map, created_test_map_errors := json_inner_create_test_response.GetMap("data")
						if created_test_map_errors != nil {
							return 
						}

						if created_test_map == nil {
							return
						}

						test_map = *created_test_map
					} else if len(*test_response_array) == 1 {
						
						test_interface := (*test_response_array)[0]
						type_of_test_interface := common.GetType(test_interface)

						if type_of_test_interface == "json.Map" {
							test_map = test_interface.(json.Map)
						} else if type_of_test_interface == "*json.Map" {
							test_map = *(test_interface.(*json.Map))
						} else {
							return
						}

					} else {
						return
					}

				test_build_branch_id, test_build_branch_id_errors := test_map.GetUInt64("test_build_branch_id")
				if test_build_branch_id_errors != nil {
					return
				}

				if test_build_branch_id == nil {
					return
				}

				create_test_log_payload := json.Map{"CreateRecord_BuildBranchInstanceStepTestResult":json.Map{"data":json.Map{"build_branch_instance_step_id":build_branch_instance_step_id,"test_build_branch_id":test_build_branch_id, "test_result_id":test_result_id}},"[queue_mode]":"PushBack","[async]":false, "[trace_id]":this_processor.GenerateTraceId()}
				create_test_log_response, create_test_log_response_errors := processor.SendMessageToQueue(&create_test_log_payload)
				if create_test_log_response_errors != nil {
					return
				}

				if create_test_log_response == nil {
					return
				}
			}
			}	
		}
	}
	return &function
}

func getStderrCallbackFunctionBranch(processor *Processor, command_name string, build_branch_id uint64, build_branch_instance_step_id uint64) (*func(message error)) {
	this_processor := processor
	//this_command_name := command_name
	//this_build_branch_id := build_branch_id
	this_build_branch_instance_step_id := build_branch_instance_step_id
	
	
	function := func(message error) {
		callback_payload := json.Map{"CreateRecord_BuildBranchInstanceStepLog":json.Map{"data":json.Map{"build_branch_instance_step_id":this_build_branch_instance_step_id,"log":fmt.Sprintf("%s",message),"stdout":false},"[queue_mode]":"PushBack","[async]":true, "[trace_id]":this_processor.GenerateTraceId()}}
		processor.SendMessageToQueueFireAndForget(&callback_payload)
	}
	return &function
}