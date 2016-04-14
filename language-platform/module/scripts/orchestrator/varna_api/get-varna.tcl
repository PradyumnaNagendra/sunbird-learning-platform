package require java
java::import -package java.util ArrayList List
java::import -package java.util HashMap Map
java::import -package com.ilimi.graph.dac.model Node

set object_type "Varna"
set error_status "Failed"

set contains_response [containsLanguage $language_id]
set contains_response_error [check_response_error $contains_response]
if {$contains_response_error} {
	puts "Error response from containsLanguage"
	return $contains_response;
}
set result [$contains_response get "result"]
set lang_eqs [$result equals "true"]
if {!$lang_eqs} {
	set result_map [java::new HashMap]
	$result_map put "code" "INVALID_LANGUAGE"
	$result_map put "message" "INVALID LANGUAGE"
	$result_map put "responseCode" [java::new Integer 400]
	set err_response [create_error_response $result_map]
	return $err_response
}
puts [$varna_id toString]
set get_node_response [getDataNode $language_id $varna_id]
set get_node_response_error [check_response_error $get_node_response]
if {$get_node_response_error} {
	puts "Error response from getDataNode"
	return $get_node_response
}

set resp_def_node [getDefinition $language_id $object_type]
set def_node [get_resp_value $resp_def_node "definition_node"]

set varna_node [get_resp_value $get_node_response "node"]
set varna_obj [convert_graph_node $varna_node $def_node]

set result_map [java::new HashMap]
$result_map put "varna" $varna_obj
set api_response [create_response $result_map]
return $api_response

