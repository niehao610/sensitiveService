package ctx

import (
	"fmt"
	"hash/crc32"
	"strconv"
)

type Method string

const (
	MethodHash = Method("hash")
)

type PartionContext struct {
	Key    string
	Method Method // hash - hash分片 global-全局
}

func (me PartionContext) String() string {
	return fmt.Sprintf("ctx:%s,%s)", me.Method, me.Key)
}

const MaxHash = 10000

// 用将字符串求出hash以分桶
func HashToUint32(s string) uint32 {
	v := crc32.ChecksumIEEE([]byte(s))
	if v >= 0 {
		return v
	} else {
		return -v
	}
}

func GetContextByDevId(devId int64) PartionContext {
	return PartionContext{Key: strconv.FormatInt(devId, 10), Method: MethodHash}
}
