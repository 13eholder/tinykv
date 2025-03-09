package raft

import "fmt"

type Debug interface {
	Printf(format string, a ...interface{})
	Println(a ...interface{})
}

type DebugImpl struct {
	enable bool
}

func NewDebug() Debug {
	return &DebugImpl{
		enable: false,
	}
}

func (d *DebugImpl) Printf(format string, a ...interface{}) {
	if d.enable {
		fmt.Printf(format, a...)
	}
}

func (d *DebugImpl) Println(a ...interface{}) {
	if d.enable {
		fmt.Println(a...)
	}
}

var _ Debug = (*DebugImpl)(nil)

var DEBUG = NewDebug()
