package redis

import (
	"fmt"
	"runtime"
	"strconv"
	"strings"
)

type Error int

const (
	ErrOK = Error(0)
	// 大于0的异常属于业务调用方错误，通常是需要调用方自行处理
	ErrPartionNotFound = Error(-1000)
)

func (me Error) detail() (string, string) {
	switch me {
	case ErrOK:
		return "OK", "正常"
	case ErrPartionNotFound:
		return "ErrPartionNotFound", "分区配置缺失，请检查配置。"
	default:
		return "Unknown", strconv.Itoa(int(me))
	}
}

// ##### 以下代码无需任何变更 #####

func (me Error) Error() string {
	s, _ := me.detail()
	return s
}

// 转整数，返回错误码时使用
func (me Error) Code() int16 { return int16(me) }

// 生成文档
func (me Error) Doc() string {
	sb := &strings.Builder{}
	_, file, _, _ := runtime.Caller(0)
	sb.WriteString(file)
	sb.WriteString("\n")
	fn := func(sb *strings.Builder, start, end, step int16) {
		for {
			e := Error(start)
			s, d := e.detail()
			if "Unknown" != s {
				sb.WriteString(fmt.Sprintf("%d %s %s\n", e.Code(), s, d))
			}
			if start == end {
				break
			} else {
				start += step
			}
		}
	}
	fn(sb, 0, 0, 1)
	fn(sb, 1, 32767, 1)
	fn(sb, -1, -32768, -1)
	return sb.String()
}
