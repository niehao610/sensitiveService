package log

import (
	"fmt"
	"log"
	"os"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"time"
)

type Level int32

func (me Level) String() string {
	switch me {
	case ALL:
		return "ALL"
	case DEBUG:
		return "DEBUG"
	case INFO:
		return "INFO"
	case WARN:
		return "WARN"
	case ERROR:
		return "ERROR"
	case FATAL:
		return "FATAL"
	case OFF:
		return "OFF"
	default:
		return fmt.Sprintf("BUG:Level:%d", me)
	}
}

const (
	ALL   = Level(0)
	DEBUG = Level(1)
	INFO  = Level(2)
	WARN  = Level(3)
	ERROR = Level(4)
	FATAL = Level(5)
	OFF   = Level(6)
)

func LogLevelDesc() string {
	desc := ""
	for i := ALL; i <= OFF; i++ {
		desc += fmt.Sprintf("%d - %s\n", i, i.String())
	}
	desc = strings.TrimSpace(desc)
	return desc
}

func SetConsole(enabled bool) {
	consoleAppender = enabled
}

func SetLevel(l Level) {
	logLevel = l
}

func GetLevel() Level {
	return logLevel
}

var logLevel Level = 1
var maxFileSize int64
var maxFileCount int32
var dailyRolling bool = true
var consoleAppender bool = true

var logObj *LogFile

const dateformat = "2006-01-02"

const (
	_        = iota
	KB int64 = 1 << (iota * 10)
	MB
	GB
	TB
)

const (
	Ldate         = 1 << iota     // the date in the local time zone: 2009/01/23
	Ltime                         // the time in the local time zone: 01:23:23
	Lmicroseconds                 // microsecond resolution: 01:23:23.123123.  assumes Ltime.
	Llongfile                     // full file name and line number: /a/b/c/d.go:23
	Lshortfile                    // final file name element and line number: d.go:23. overrides Llongfile
	LUTC                          // if Ldate or Ltime is set, use UTC rather than the local time zone
	LstdFlags     = Ldate | Ltime // initial values for the standard logger
)

func Log(l Level, logprefix string, callerdep int, objs ...interface{}) {
	if l > INFO {
		MetricsExceptionInc(l.String())
	}
	if logLevel <= l {
		if dailyRolling {
			fileCheck()
		}
		defer catchError()
		str := l.String() + " " + logprefix + " " + fmt.Sprintln(objs...)

		if consoleAppender {
			console(callerdep, str)
		}

		if logObj != nil {
			if logObj.mmap == nil {
				if logObj.mu != nil && logObj.lg != nil && logObj.mu != nil {
					logObj.mu.RLock()
					defer logObj.mu.RUnlock()
					logObj.lg.Output(callerdep, str)
				}
			} else {
				logObj.OutputMMap(callerdep, str)
			}
		}
	}
}

func (f *LogFile) OutputFile(calldepth int, s string) {
	if f.mu != nil && f.lg != nil && f.mu != nil {
		f.mu.RLock()
		defer f.mu.RUnlock()
		f.lg.Output(calldepth, s)
	}
}

func (f *LogFile) OutputMMap(calldepth int, s string) {
	now := time.Now() // get this early.
	var file string
	var line int
	if f.flag&(Lshortfile|Llongfile) != 0 {
		var ok bool
		_, file, line, ok = runtime.Caller(calldepth)
		if !ok {
			file = "???"
			line = 0
		}
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.buf = f.buf[:0]
	f.formatHeader(&f.buf, now, file, line)
	f.buf = append(f.buf, s...)
	if len(s) == 0 || s[len(s)-1] != '\n' {
		f.buf = append(f.buf, '\n')
	}

	if f.mmap != nil {
		f.mmap.write(f.buf)
	}

	return
}

func (f *LogFile) formatHeader(buf *[]byte, t time.Time, file string, line int) {
	*buf = append(*buf, f.prefix...)
	if f.flag&(Ldate|Ltime|Lmicroseconds) != 0 {
		if f.flag&LUTC != 0 {
			t = t.UTC()
		}
		if f.flag&Ldate != 0 {
			year, month, day := t.Date()
			itoa(buf, year, 4)
			*buf = append(*buf, '/')
			itoa(buf, int(month), 2)
			*buf = append(*buf, '/')
			itoa(buf, day, 2)
			*buf = append(*buf, ' ')
		}
		if f.flag&(Ltime|Lmicroseconds) != 0 {
			hour, min, sec := t.Clock()
			itoa(buf, hour, 2)
			*buf = append(*buf, ':')
			itoa(buf, min, 2)
			*buf = append(*buf, ':')
			itoa(buf, sec, 2)
			if f.flag&Lmicroseconds != 0 {
				*buf = append(*buf, '.')
				itoa(buf, t.Nanosecond()/1e3, 6)
			}
			*buf = append(*buf, ' ')
		}
	}
	if f.flag&(Lshortfile|Llongfile) != 0 {
		if f.flag&Lshortfile != 0 {
			short := file
			for i := len(file) - 1; i > 0; i-- {
				if file[i] == '/' {
					short = file[i+1:]
					break
				}
			}
			file = short
		}
		*buf = append(*buf, file...)
		*buf = append(*buf, ':')
		itoa(buf, line, -1)
		*buf = append(*buf, ": "...)
	}
}

func itoa(buf *[]byte, i int, wid int) {
	// Assemble decimal in reverse order.
	var b [20]byte
	bp := len(b) - 1
	for i >= 10 || wid > 1 {
		wid--
		q := i / 10
		b[bp] = byte('0' + i - q*10)
		bp--
		i = q
	}
	// i < 10
	b[bp] = byte('0' + i)
	*buf = append(*buf, b[bp:]...)
}

type LogFile struct {
	dir      string
	filename string
	_suffix  int
	isCover  bool
	_date    *time.Time
	mu       *sync.RWMutex
	logfile  *os.File
	lg       *log.Logger
	prefix   string
	flag     int
	buf      []byte

	mmap *Mmap
}

func LoggerSetRollingDaily(fileDir, fileName, tmpFileName string) {
	dailyRolling = true
	t, _ := time.Parse(dateformat, time.Now().Format(dateformat))
	logObj = &LogFile{dir: fileDir, filename: fileName, _date: &t, isCover: false, mu: new(sync.RWMutex)}

	//check rename
	logObj.mu.Lock()
	if !logObj.isMustRename() {
		logObj.logfile, _ = os.OpenFile(fileDir+"/"+fileName, os.O_RDWR|os.O_APPEND|os.O_CREATE, os.FileMode(0644))
		if tmpFileName == "" {
			logObj.lg = log.New(logObj.logfile, "\n", log.Ldate|log.Ltime|log.Lshortfile)
		} else {
			logObj.mmap, _ = NewMmap(fileDir+"/"+tmpFileName, defaultTmpLogPagesize)
			logObj.prefix = "\n"
			logObj.flag = Ldate | Ltime | Lshortfile
		}
	} else {
		logObj.rename()
	}
	logObj.mu.Unlock()
}

const (
	FlagDate   = Ldate | Ltime
	FlagNormal = Ldate | Ltime | Lshortfile
)

// 独立的logger 单独的文件
func NewLogger(fileDir, fileName, tmpFileName string, flag int) *LogFile {
	t, _ := time.Parse(dateformat, time.Now().Format(dateformat))
	mylog := &LogFile{dir: fileDir, filename: fileName, _date: &t, isCover: false, mu: new(sync.RWMutex)}
	if !mylog.isMustRename() {
		mylog.logfile, _ = os.OpenFile(fileDir+"/"+fileName, os.O_RDWR|os.O_APPEND|os.O_CREATE, os.FileMode(0644))
		if tmpFileName == "" {
			mylog.lg = log.New(mylog.logfile, "\n", flag)
		} else {
			mylog.mmap, _ = NewMmap(fileDir+"/"+tmpFileName, defaultTmpLogPagesize)
			mylog.prefix = "\n"
			mylog.flag = flag
		}
	} else {
		mylog.rename()
	}
	return mylog
}

func console(callerdep int, s ...interface{}) {
	_, file, line, _ := runtime.Caller(callerdep)
	short := file
	for i := len(file) - 1; i > 0; i-- {
		if file[i] == '/' {
			short = file[i+1:]
			break
		}
	}
	file = short
	log.Println(file + ":" + strconv.Itoa(line) + " " + fmt.Sprint(s...))
}

func catchError() {
	if err := recover(); err != nil {
		log.Println("err", err)
	}
}

func (f *LogFile) isMustRename() bool {
	if dailyRolling {
		t, _ := time.Parse(dateformat, time.Now().Format(dateformat))
		if t.After(*f._date) {
			return true
		}
	} else {
		if maxFileCount > 1 {
			if fileSize(f.dir+"/"+f.filename) >= maxFileSize {
				return true
			}
		}
	}
	return false
}

func (f *LogFile) rename() {
	if dailyRolling {
		fn := f.dir + "/" + f.filename + "." + f._date.Format(dateformat)
		if !isExist(fn) && f.isMustRename() {
			if f.logfile != nil {
				f.logfile.Close()
			}
			err := os.Rename(f.dir+"/"+f.filename, fn)
			if err != nil {
				f.lg.Println("rename err", err.Error())
			}
			t, _ := time.Parse(dateformat, time.Now().Format(dateformat))
			f._date = &t
			f.logfile, _ = os.Create(f.dir + "/" + f.filename)
			f.lg = log.New(logObj.logfile, "\n", log.Ldate|log.Ltime|log.Lshortfile)
		}
	} else {
		f.coverNextOne()
	}
}

func (f *LogFile) nextSuffix() int {
	return int(f._suffix%int(maxFileCount) + 1)
}

func (f *LogFile) coverNextOne() {
	f._suffix = f.nextSuffix()
	if f.logfile != nil {
		f.logfile.Close()
	}
	if isExist(f.dir + "/" + f.filename + "." + strconv.Itoa(int(f._suffix))) {
		os.Remove(f.dir + "/" + f.filename + "." + strconv.Itoa(int(f._suffix)))
	}
	os.Rename(f.dir+"/"+f.filename, f.dir+"/"+f.filename+"."+strconv.Itoa(int(f._suffix)))
	f.logfile, _ = os.Create(f.dir + "/" + f.filename)
	f.lg = log.New(logObj.logfile, "\n", log.Ldate|log.Ltime|log.Lshortfile)
}

func fileSize(file string) int64 {
	f, e := os.Stat(file)
	if e != nil {
		fmt.Fprintln(os.Stderr, "framework - logger fileSize()", e.Error())
		return 0
	}
	return f.Size()
}

func isExist(path string) bool {
	_, err := os.Stat(path)
	return err == nil || os.IsExist(err)
}

func fileCheck() {
	defer func() {
		if err := recover(); err != nil {
			log.Println(err)
		}
	}()
	if logObj != nil && logObj.isMustRename() {
		logObj.mu.Lock()
		defer logObj.mu.Unlock()
		logObj.rename()
	}
}

func SetRollingDaily(fileDir, fileName string) {
	LoggerSetRollingDaily(fileDir, fileName, "")
}

func All(objs ...interface{}) {
	Log(ALL, "", 3, objs...)
}

func Info(objs ...interface{}) {
	Log(INFO, "", 3, objs...)
}

func Debug(objs ...interface{}) {
	Log(DEBUG, "", 3, objs...)
}

func Warn(objs ...interface{}) {
	Log(WARN, "", 3, objs...)
}

func Error(objs ...interface{}) {
	Log(ERROR, "", 3, objs...)
}

func Fatal(objs ...interface{}) {
	Log(FATAL, "", 3, objs...)
}

func Close() {
	if logObj.mmap != nil {
		logObj.mmap.Close()
	}
}
