package er

import (
	"errors"
	"fmt"
	"net/http"
	"runtime"
	"strings"
)

type Kind int

const (
	KindUndefined Kind = iota
	KindInternalServerError
	KindBadRequest
	KindNotFound
	KindFatal
	KindConflict
	KindTimeout
)

var (
	mapKindTohttpStatus = map[Kind]int{
		KindUndefined:           http.StatusInternalServerError,
		KindInternalServerError: http.StatusInternalServerError,
		KindBadRequest:          http.StatusBadRequest,
		KindNotFound:            http.StatusNotFound,
		KindFatal:               http.StatusInternalServerError,
		KindConflict:            http.StatusConflict,
		KindTimeout:             http.StatusRequestTimeout,
	}
)

type Error struct {
	Ops  []string
	Kind Kind
	Err  error
}

func New(errMsg, op string, kind Kind) error {
	return WrapOpAndKind(errors.New(errMsg), op, kind)
}

func new(err error) *Error {
	if e, ok := err.(*Error); ok {
		return e
	}
	return &Error{Err: err, Kind: KindUndefined}
}

func Parse(err error) *Error {
	return new(err)
}

func NewEmpty(op string) *Error {
	return &Error{
		Ops: []string{op},
	}
}

func Is(sourceErr, targetErr error) bool {
	se := new(sourceErr)
	te := new(targetErr)

	return errors.Is(se.Err, te.Err)
}

func WrapOp(err error, op string) error {
	e := new(err)
	e.Ops = append(e.Ops, op)
	return e
}

func WrapKindIfNotSet(err error, kind Kind) error {
	e := new(err)
	if e.Kind != KindUndefined {
		return e
	}
	e.Kind = kind
	return e
}

func WrapKind(err error, kind Kind) error {
	e := new(err)
	e.Kind = kind
	return e
}

func IsKind(err error, kind Kind) bool {
	e := new(err)
	return e.Kind == kind
}

func WrapOpAndKind(err error, op string, kind Kind) error {
	e := new(err)
	e.Ops = append(e.Ops, op)
	e.Kind = kind
	return e
}

func KindToHTTPStatus(kind Kind) int {
	if v, ok := mapKindTohttpStatus[kind]; ok {
		return v
	}
	return mapKindTohttpStatus[KindUndefined]
}

func (e *Error) Error() string {
	if e.Err == nil {
		e.Err = errors.New("")
	}
	ops := []string{e.Err.Error()}
	ops = append(ops, e.Ops...)
	return strings.Join(ops, "\n")
}

func GetOperator() string {
	pc, _, _, _ := runtime.Caller(1)
	caller := runtime.FuncForPC(pc).Name()
	splits := strings.Split(caller, "/")
	return strings.Join(splits[3:], ".")
}

func PanicIfErrorWithOp(err error, op string) {
	if err != nil {
		err = WrapOp(err, op)
		panic(fmt.Errorf("%v", err.Error()))
	}
}
