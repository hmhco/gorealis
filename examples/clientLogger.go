/**
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package main

import "os"

type logger interface {
	Println(v ...interface{})
	Printf(format string, v ...interface{})
	Print(v ...interface{})
}

// LevelLogger is a logger that can be configured to output different levels of information: Debug and Trace.
// Trace should only be enabled when very in depth information about the sequence of events a function took is needed.
type LevelLogger struct {
	logger
	debug bool
	trace bool
}

func (l LevelLogger) errorPrintf(format string, a ...interface{}) {
	l.Printf("[ERROR] "+format, a...)
}

func (l LevelLogger) errorPrint(a ...interface{}) {
	l.Print(append([]interface{}{"[ERROR] "}, a...)...)
}

func (l LevelLogger) errorPrintln(a ...interface{}) {
	l.Println(append([]interface{}{"[ERROR] "}, a...)...)
}

func (l LevelLogger) fatalPrintf(format string, a ...interface{}) {
	l.Printf("[FATAL] "+format, a...)
	os.Exit(1)
}

func (l LevelLogger) fatalPrint(a ...interface{}) {
	l.Print(append([]interface{}{"[FATAL] "}, a...)...)
	os.Exit(1)
}

func (l LevelLogger) fatalPrintln(a ...interface{}) {
	l.Println(append([]interface{}{"[FATAL] "}, a...)...)
	os.Exit(1)
}

func (l LevelLogger) zfatalPrintf(format string, a ...interface{}) {
	l.Printf("[ZFATAL] "+format, a...)
	os.Exit(0)
}

func (l LevelLogger) zfatalPrint(a ...interface{}) {
	l.Print(append([]interface{}{"[ZFATAL] "}, a...)...)
	os.Exit(0)
}

func (l LevelLogger) zfatalPrintln(a ...interface{}) {
	l.Println(append([]interface{}{"[ZFATAL] "}, a...)...)
	os.Exit(0)
}

func (l LevelLogger) panicPrintf(format string, a ...interface{}) {
	l.Printf("[PANIC] "+format, a...)
	panic(a)
}

func (l LevelLogger) panicPrint(a ...interface{}) {
	l.Print(append([]interface{}{"[PANIC] "}, a...)...)
	panic(a)
}

func (l LevelLogger) panicPrintln(a ...interface{}) {
	l.Println(append([]interface{}{"[PANIC] "}, a...)...)
	panic(a)
}

// EnableDebug enables debug level logging for the LevelLogger
func (l *LevelLogger) EnableDebug(enable bool) {
	l.debug = enable
}

// EnableTrace enables trace level logging for the LevelLogger
func (l *LevelLogger) EnableTrace(enable bool) {
	l.trace = enable
}

func (l LevelLogger) debugPrintf(format string, a ...interface{}) {
	if l.debug {
		l.Printf("[DEBUG] "+format, a...)
	}
}

func (l LevelLogger) debugPrint(a ...interface{}) {
	if l.debug {
		l.Print(append([]interface{}{"[DEBUG] "}, a...)...)
	}
}

func (l LevelLogger) debugPrintln(a ...interface{}) {
	if l.debug {
		l.Println(append([]interface{}{"[DEBUG] "}, a...)...)
	}
}

func (l LevelLogger) tracePrintf(format string, a ...interface{}) {
	if l.trace {
		l.Printf("[TRACE] "+format, a...)
	}
}

func (l LevelLogger) tracePrint(a ...interface{}) {
	if l.trace {
		l.Print(append([]interface{}{"[TRACE] "}, a...)...)
	}
}

func (l LevelLogger) tracePrintln(a ...interface{}) {
	if l.trace {
		l.Println(append([]interface{}{"[TRACE] "}, a...)...)
	}
}
