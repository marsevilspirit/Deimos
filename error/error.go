package error

import (
	"encoding/json"
	"fmt"
	"net/http"
)

var errors map[int]string

const (
	EcodeKeyNotFound    = 100
	EcodeTestFailed     = 101
	EcodeNotFile        = 102
	EcodeNoMoreMachine  = 103
	EcodeNotDir         = 104
	EcodeNodeExist      = 105
	EcodeKeyIsPreserved = 106
	EcodeRootROnly      = 107
	EcodeDirNotEmpty    = 108

	EcodeValueRequired      = 200
	EcodePrevValueRequired  = 201
	EcodeTTLNaN             = 202
	EcodeIndexNaN           = 203
	EcodeValueOrTTLRequired = 204

	EcodeRaftInternal = 300
	EcodeLeaderElect  = 301

	EcodeWatcherCleared    = 400
	EcodeEventIndexCleared = 401
)

func init() {
	errors = make(map[int]string)

	// command related errors
	errors[EcodeKeyNotFound] = "Key Not Found"
	errors[EcodeTestFailed] = "Compare failed" //test and set
	errors[EcodeNotFile] = "Not a file"
	errors[EcodeNoMoreMachine] = "Reached the max number of machines in the cluster"
	errors[EcodeNotDir] = "Not A Directory"
	errors[EcodeNodeExist] = "Key Already exists" // create
	errors[EcodeRootROnly] = "Root is read only"
	errors[EcodeKeyIsPreserved] = "The prefix of given key is a keyword in mars"
	errors[EcodeDirNotEmpty] = "The directory is not empty"

	// Post form related errors
	errors[EcodeValueRequired] = "Value is Required in POST form"
	errors[EcodePrevValueRequired] = "PrevValue is Required in POST form"
	errors[EcodeTTLNaN] = "The given TTL in POST form is not a number"
	errors[EcodeIndexNaN] = "The given index in POST form is not a number"
	errors[EcodeValueOrTTLRequired] = "Value or TTL is required in POST form"

	// raft related errors
	errors[EcodeRaftInternal] = "Raft Internal Error"
	errors[EcodeLeaderElect] = "During Leader Election"

	// mars related errors
	errors[EcodeWatcherCleared] = "watcher is cleared due to mars recovery"
	errors[EcodeEventIndexCleared] = "The event in requested index is outdated and cleared"
}

type Error struct {
	ErrorCode int    `json:"errorCode"`
	Message   string `json:"message"`
	Cause     string `json:"cause,omitempty"`
	Index     uint64 `json:"index"`
}

func NewError(errorCode int, cause string, index uint64) *Error {
	return &Error{
		ErrorCode: errorCode,
		Message:   errors[errorCode],
		Cause:     cause,
		Index:     index,
	}
}

func Message(code int) string {
	return errors[code]
}

// Only for error interface
func (e Error) Error() string {
	return e.Message
}

func (e Error) toJsonString() string {
	b, _ := json.Marshal(e)
	return string(b)
}

func (e Error) Write(w http.ResponseWriter) {
	w.Header().Add("X-Marstore-Index", fmt.Sprint(e.Index))
	// 3xx is reft internal error
	if e.ErrorCode/100 == 3 {
		http.Error(w, e.toJsonString(), http.StatusInternalServerError)
	} else {
		http.Error(w, e.toJsonString(), http.StatusBadRequest)
	}
}
