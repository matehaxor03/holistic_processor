package processor

import (
	json "github.com/matehaxor03/holistic_json/json"
	common "github.com/matehaxor03/holistic_common/common"
	"strings"
	"fmt"
)

func getStdoutCallbackFunctionBranch(processor *Processor, command_name string, branch_instance_id uint64, branch_instance_step_id uint64, branch_id uint64, label string) (*func(message string)) {
	this_processor := processor
	this_command_name := command_name
	this_branch_instance_step_id := branch_instance_step_id
	this_branch_id := branch_id
	this_label := label
	
	function := func(message string) {
		callback_payload_map_data :=  map[string]interface{}{"branch_instance_id":branch_instance_id, "branch_instance_step_id":this_branch_instance_step_id,"log":message,"stdout":true}
		callback_payload_data :=  json.NewMapOfValues(&callback_payload_map_data)

		callback_payload_map := map[string]interface{}{"[queue]":"CreateRecord_BranchInstanceStepLog", "[queue_mode]":"PushBack", "[async]":true, "[trace_id]":this_processor.GenerateTraceId()}
		callback_payload := json.NewMapOfValues(&callback_payload_map)
		callback_payload.SetMap("data", callback_payload_data)

		go this_processor.SendMessageToQueueFireAndForget(callback_payload)

		if this_command_name != "Run_IntegrationTestSuite" {
			return
		}
		
		message = strings.ReplaceAll(message, "\\\"", "\"")
		json_message, json_message_errors := json.Parse(message)
		if json_message_errors != nil {
			fmt.Println(json_message_errors)
			return
		}

		if json_message == nil {
			fmt.Println("json_message is nil")
			return
		}

		test_value, test_value_errors := json_message.GetString("Test")

		if test_value_errors != nil {
			fmt.Println(test_value_errors)
			return
		}

		if test_value == nil {
			return
		}

		test_result_value, test_result_value_errors := json_message.GetString("Action")

		if test_result_value_errors != nil {
			fmt.Println(test_result_value_errors)
			return
		}

		if test_result_value == nil {
			fmt.Println("test_result_value is nil")
			return
		}

		if *test_result_value == "pass" {
			*test_result_value = "Passed"
		} else if *test_result_value == "fail" {
			*test_result_value = "Failed"
		} else {
			return
		}

		elapsed_value, elapsed_value_errors := json_message.GetFloat64("Elapsed")
		if elapsed_value_errors != nil {
			fmt.Println(elapsed_value_errors)
			return
		} else if common.IsNil(elapsed_value) {
			fmt.Println("elapsed_value is nil")
			return
		}

		read_records_test_suite_where_array := json.NewArray()
		
		read_records_test_suite_where_branch_id := map[string]interface{}{"column":"branch_id","value":this_branch_id,"logic":"="}
		read_records_test_suite_where_map_branch_id := json.NewMapOfValues(&read_records_test_suite_where_branch_id)
		
		read_records_test_suite_where_name := map[string]interface{}{"column":"name","value":this_label,"logic":"="}
		read_records_test_suite_where_map_name := json.NewMapOfValues(&read_records_test_suite_where_name)

		read_records_test_suite_where_array.AppendMap(read_records_test_suite_where_map_branch_id)
		read_records_test_suite_where_array.AppendMap(read_records_test_suite_where_map_name)

		read_records_test_suite_request := map[string]interface{}{"[queue]":"ReadRecords_TestSuiteBranch", "[limit]":1,"[queue_mode]":"PushBack","[async]":false,"[trace_id]":this_processor.GenerateTraceId()}
		read_records_test_suite_request_map := json.NewMapOfValues(&read_records_test_suite_request)
		read_records_test_suite_request_map.SetArray("[where_fields]", read_records_test_suite_where_array)

		test_suite_response, test_suite_response_errors := this_processor.SendMessageToQueue(read_records_test_suite_request_map)
		if test_suite_response_errors != nil {
			fmt.Println(test_suite_response_errors)
			return
		} else if common.IsNil(test_suite_response) {
			fmt.Println("test_suite_response is nil")
			return
		}

		test_suite_response_array, test_suite_response_array_errors := test_suite_response.GetArray("data")
		if test_suite_response_array_errors != nil {
			fmt.Println(test_suite_response_array_errors)
			return
		} else if common.IsNil(test_suite_response_array) {
			fmt.Println("test_suite_response_array is nil")
			return
		}

		var test_suite_map json.Map
		if len(*(test_suite_response_array.GetValues())) == 0 {
			create_test_suite_payload_map_data :=  map[string]interface{}{"branch_id":this_branch_id,"name":this_label}
			create_test_suite_payload_data :=  json.NewMapOfValues(&create_test_suite_payload_map_data)

			create_test_suite_payload_map := map[string]interface{}{"[queue]":"CreateRecord_TestSuiteBranch", "[queue_mode]":"PushBack", "[async]":true, "[trace_id]":this_processor.GenerateTraceId()}
			create_test_suite_payload := json.NewMapOfValues(&create_test_suite_payload_map)
			create_test_suite_payload.SetMap("data", create_test_suite_payload_data)
			
			create_suite_response, create_suite_response_errors := this_processor.SendMessageToQueue(create_test_suite_payload)
			if create_suite_response_errors != nil {
				if !(len(create_suite_response_errors) == 1 && strings.Contains(fmt.Sprintf("%s", create_suite_response_errors[0]), "Duplicate entry")) { 
					fmt.Println(create_suite_response_errors)
					return
				} else {
					test_suite_response, test_suite_response_errors = this_processor.SendMessageToQueue(read_records_test_suite_request_map)
					if test_suite_response_errors != nil {
						fmt.Println(test_suite_response_errors)
						return
					} else if common.IsNil(test_suite_response) {
						fmt.Println("test_suite_response is nil")
						return
					}

					test_suite_response_array, test_suite_response_array_errors = test_suite_response.GetArray("data")
					if test_suite_response_array_errors != nil {
						fmt.Println(test_suite_response_array_errors)
						return
					} else if common.IsNil(test_suite_response_array) {
						fmt.Println("test_suite_response_array is nil")
						return
					} else if len(*(test_suite_response_array.GetValues())) != 1 {
						fmt.Println("test_suite_response_array not one record")
						return
					}
				}
			} else if common.IsNil(create_suite_response) {
				fmt.Println("create_suite_response is nil")
				return
			} else {
				created_test_suite_map, created_test_suite_map_errors := create_suite_response.GetMap("data")
				if created_test_suite_map_errors != nil {
					fmt.Println(created_test_suite_map_errors)
					return 
				} else if common.IsNil(created_test_suite_map) {
					fmt.Println("created_test_suite_map is nil")
					return
				}

				test_suite_response_array.AppendMap(created_test_suite_map)
			}
		} 
		
		
		if len(*(test_suite_response_array.GetValues())) == 1 {
			temp_test_suite_map, temp_test_suite_map_errors := (*(test_suite_response_array.GetValues()))[0].GetMap()

			if temp_test_suite_map_errors != nil {
				fmt.Println(temp_test_suite_map_errors)
				return 
			} else if common.IsNil(temp_test_suite_map) {
				fmt.Println("test_suite_map is nil")
				return
			}
			
			test_suite_map = *temp_test_suite_map
		} else {
			fmt.Println("test_suite_response_array did not return 1 or 0 records")
			return
		}

		test_suite_branch_id, test_suite_branch_id_errors := test_suite_map.GetUInt64("test_suite_branch_id")
		if test_suite_branch_id_errors != nil {
			fmt.Println(test_suite_branch_id_errors)
			return
		} else if common.IsNil(test_suite_branch_id) {
			fmt.Println("test_suite_branch_id is nil")
			return
		}

		read_records_test_where_array := json.NewArray()
		
		read_records_test_where_test_suite_branch_id := map[string]interface{}{"column":"test_suite_branch_id","value":*test_suite_branch_id,"logic":"="}
		read_records_test_where_map_test_suite_branch_id := json.NewMapOfValues(&read_records_test_where_test_suite_branch_id)

		read_records_test_where_name := map[string]interface{}{"column":"name","value":*test_value, "logic":"="}
		read_records_test_where_map_name := json.NewMapOfValues(&read_records_test_where_name)

		read_records_test_where_array.AppendMap(read_records_test_where_map_test_suite_branch_id)
		read_records_test_where_array.AppendMap(read_records_test_where_map_name)

		read_records_test_request := map[string]interface{}{"[queue]":"ReadRecords_TestBranch", "[limit]":1,"[queue_mode]":"PushBack","[async]":false, "[trace_id]":this_processor.GenerateTraceId()}
		read_records_test_request_map := json.NewMapOfValues(&read_records_test_request)
		read_records_test_request_map.SetArray("[where_fields]", read_records_test_where_array)

		test_response, test_response_errors := this_processor.SendMessageToQueue(read_records_test_request_map)
		if test_response_errors != nil {
			fmt.Println(test_response_errors)
			return
		} else if common.IsNil(test_response) {
			fmt.Println("test_response is nil")
			return
		}

		test_response_array, test_response_array_errors := test_response.GetArray("data")
		if test_response_array_errors != nil {
			fmt.Println(test_response_array_errors)
			return
		} else if common.IsNil(test_response_array) {
			fmt.Println("test_response_array is nil")
			return
		}

		var test_map json.Map
		if len(*(test_response_array.GetValues())) == 0 {
			create_test_payload_map_data :=  map[string]interface{}{"test_suite_branch_id":test_suite_branch_id,"name":*test_value}
			create_test_payload_data := json.NewMapOfValues(&create_test_payload_map_data)

			create_test_payload_map := map[string]interface{}{"[queue]":"CreateRecord_TestBranch","[queue_mode]":"PushBack","[async]":false, "[trace_id]":this_processor.GenerateTraceId()}
			create_test_payload := json.NewMapOfValues(&create_test_payload_map)
			create_test_payload.SetMap("data", create_test_payload_data)

			create_test_response, create_test_response_errors := this_processor.SendMessageToQueue(create_test_payload)
			if create_test_response_errors != nil {
				if !(len(create_test_response_errors) == 1 && strings.Contains(fmt.Sprintf("%s", create_test_response_errors[0]), "Duplicate entry")) { 
					fmt.Println(create_test_response_errors)
					return
				} else {
					test_response, test_response_errors := this_processor.SendMessageToQueue(read_records_test_request_map)
					if test_response_errors != nil {
						fmt.Println(test_response_errors)
						return
					} else if common.IsNil(test_response) {
						fmt.Println("test_response is nil")
						return
					}
	
					test_response_array, test_response_array_errors = test_response.GetArray("data")
					if test_response_array_errors != nil {
						fmt.Println(test_response_array_errors)
						return
					} else if common.IsNil(test_response_array) {
						fmt.Println("test_response_array is nil")
						return
					} else if len(*(test_response_array.GetValues())) != 1 {
						fmt.Println("test_response_array not one record")
						return
					}
				}
			} else if common.IsNil(create_test_response) {
				fmt.Println("create_test_response is nil")
				return
			} else {
				created_test_map, created_test_map_errors := create_test_response.GetMap("data")
				if created_test_map_errors != nil {
					fmt.Println(created_test_map_errors)
					return 
				} else if common.IsNil(created_test_map) {
					fmt.Println("created_test_suite_map is nil")
					return
				}

				test_response_array.AppendMap(created_test_map)
			}
		} 
		
	if len(*(test_response_array.GetValues())) == 1 {
		temp_test_map, temp_test_map_errors := (*(test_response_array.GetValues()))[0].GetMap()
		if temp_test_map_errors != nil {
			fmt.Println(temp_test_map_errors)
			return 
		} else if common.IsNil(temp_test_map) {
			fmt.Println("temp_test_map is nil")
			return
		}
		test_map = *temp_test_map
	} else {
		fmt.Println("test_response_array returned more than 1 record")
		return
	}

	test_branch_id, test_branch_id_errors := test_map.GetUInt64("test_branch_id")
	if test_branch_id_errors != nil {
		fmt.Println(test_branch_id_errors)
		return
	} else if common.IsNil(test_branch_id) {
		fmt.Println("test_branch_id is nil")
		return
	}
	
	select_test_result_where_array := json.NewArray()
	
	select_test_result_where := map[string]interface{}{"column":"name", "value":*test_result_value, "logic":"="}
	select_test_result_where_map := json.NewMapOfValues(&select_test_result_where)
	select_test_result_where_array.AppendMap(select_test_result_where_map)

	select_test_result_request := map[string]interface{}{"[queue]":"ReadRecords_TestResult", "[limit]":1,"[queue_mode]":"PushBack","[async]":false, "[trace_id]":this_processor.GenerateTraceId()}
	select_test_result_request_map := json.NewMapOfValues(&select_test_result_request)
	select_test_result_request_map.SetArray("[where_fields]", select_test_result_where_array)

	test_result_response, test_result_response_errors := this_processor.SendMessageToQueue(select_test_result_request_map)
	if test_result_response_errors != nil {
		fmt.Println(test_result_response_errors)
		return
	} else if common.IsNil(test_result_response) {
		fmt.Println("test_result_response is nil")
		return
	}

	test_result_response_array, test_result_response_array_errors := test_result_response.GetArray("data")
	if test_result_response_array_errors != nil {
		fmt.Println(test_result_response_array_errors)
		return
	} else if common.IsNil(test_result_response_array) {
		fmt.Println("test_result_response_array is nil")
		return
	}

	var test_result_map json.Map
	if len(*(test_result_response_array.GetValues())) == 1 {
		temp_test_result_map, temp_test_result_map_errors := (*(test_result_response_array.GetValues()))[0].GetMap()
		if temp_test_result_map_errors != nil {
			fmt.Println(temp_test_result_map_errors)
			return
		} else if common.IsNil(temp_test_result_map) {
			fmt.Println("temp_test_result_map is nil")
			return
		}
		test_result_map = *temp_test_result_map
	} else {
		fmt.Println("test_result_response_array did not have 1 record")
		return
	}

	test_result_id, test_result_id_errors := test_result_map.GetUInt64("test_result_id")
	if test_result_id_errors != nil {
		fmt.Println(test_result_id_errors)
		return 
	} else if common.IsNil(test_result_id) {
		fmt.Println("test_result_id is nil")
		return
	}

	create_test_log_payload_map_data :=  map[string]interface{}{"branch_instance_step_id":branch_instance_step_id,"test_branch_id":*test_branch_id, "test_result_id":*test_result_id, "duration":*elapsed_value}
	create_test_log_payload_data := json.NewMapOfValues(&create_test_log_payload_map_data)

	create_test_log_payload_map := map[string]interface{}{"[queue]":"CreateRecord_BranchInstanceStepTestResult","[queue_mode]":"PushBack","[async]":true, "[trace_id]":this_processor.GenerateTraceId()}
	create_test_log_payload := json.NewMapOfValues(&create_test_log_payload_map)
	create_test_log_payload.SetMap("data", create_test_log_payload_data)

	go this_processor.SendMessageToQueueFireAndForget(create_test_log_payload)
	
	
	}	
	return &function
}

func getStderrCallbackFunctionBranch(processor *Processor, command_name string, branch_instance_id uint64, branch_instance_step_id uint64, branch_id uint64, label string) (*func(message error)) {
	this_processor := processor
	this_branch_instance_step_id := branch_instance_step_id
	
	
	function := func(message error) {
		callback_payload_map_data :=  map[string]interface{}{"branch_instance_id":branch_instance_id, "branch_instance_step_id":this_branch_instance_step_id,"log":fmt.Sprintf("%s",message),"stdout":false}
		callback_payload_data :=  json.NewMapOfValues(&callback_payload_map_data)

		callback_payload_map := map[string]interface{}{"[queue]":"CreateRecord_BranchInstanceStepLog", "[queue_mode]":"PushBack", "[async]":true, "[trace_id]":this_processor.GenerateTraceId()}
		callback_payload := json.NewMapOfValues(&callback_payload_map)
		callback_payload.SetMap("data", callback_payload_data)

		go this_processor.SendMessageToQueueFireAndForget(callback_payload)
	}
	return &function
}