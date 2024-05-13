package main

import (
	"reflect"
	"testing"
)

func TestUnmarshalJsonFromRedisStreamParam(t *testing.T) {
	// Real example from binary_websocket_api
	param1 := "{\"source_bypass_verification\":0,\"language\":\"EN\",\"source\":\"1\",\"brand\":\"deriv\",\"token\":\"a1-5tSSSkbu3gYzAyOBKDU1cXH35r0lo\",\"args\":{\"get_notifications\":1,\"req_id\":2,\"subscribe\":1},\"valid_source\":\"1\",\"logging\":{},\"account_tokens\":{\"CR90000000\":{\"token\":\"a1-5tSSSkbu3gYzAyOBKDU1cXH35r0lo\",\"broker\":\"CR\",\"app_id\":\"16303\",\"is_virtual\":0}},\"country_code\":\"de\"}"

	result1, err1 := unmarshalJsonFromRedisStreamParam(param1, map[string]any{})

	if err1 != nil {
		t.Errorf("Unexpected error: %v", err1)
	}
	if result1["language"] != "EN" {
		t.Errorf("Expected EN, but got %v", result1["language"])
	}

	// Test case 2: Empty JSON object
	param2 := `{}`
	expected2 := map[string]interface{}{}
	result2, err2 := unmarshalJsonFromRedisStreamParam(param2, map[string]any{})
	if err2 != nil {
		t.Errorf("Unexpected error: %v", err2)
	}
	if !reflect.DeepEqual(result2, expected2) {
		t.Errorf("Expected %v, but got %v", expected2, result2)
	}

	// Test case 3: Invalid JSON object
	param3 := `{"name": "John", "age": "thirty"`
	_, err3 := unmarshalJsonFromRedisStreamParam(param3, map[string]any{})
	if err3 == nil {
		t.Error("Expected an error, but got nil")
	}
}
