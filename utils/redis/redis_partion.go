package redis

import "C"
import (
	"encoding/json"
	"sensitiveService/utils/ctx"
	logger "sensitiveService/utils/log"
	"strings"
	"time"
)

// 原理
// 通过将key hash 到 1w个桶里面  1w个桶按区段分配到固定的库里

// XXX  找到所有需要改造的点 目前找到75个
// grep -rn  -E '\.Redis[a-zA-Z0-9]+\(' *|grep -v framework.|wc -l
/* 需要支持的方法才15个
woo:jumscore woo$ grep -h -r  -E -o '\.(Redis[a-zA-Z0-9]+)\(.*\)' *|grep -v framework|awk -F \( '{print $1}'|sort|uniq
.RedisDel
.RedisExists
.RedisExpire
.RedisGet
.RedisGetOrExit
.RedisHDel
.RedisHExists
.RedisHGet
.RedisHGetAll
.RedisHMSet
.RedisHSet
.RedisHincrby
.RedisSet
.RedisSet2
.RedisSetEx
woo:jumscore woo$ grep -h -r  -E -o '\.(Redis[a-zA-Z0-9]+)\(.*\)' *|grep -v framework|awk -F \( '{print $1}'|sort|uniq|wc -l
      15
*/

const key_mid_fix = "-p-"

type HashRange struct {
	PartionName string // 分区名 也许后续根据分区 将MQ 网络 主机等等资源全部隔离 这里PartionName就像MQ里的Rkey一样用来区分
	Start       int64  // 左包含
	End         int64  // 右包含
}

func (me HashRange) Has(h int64) bool {
	return me.Start <= h && h <= me.End
}

type Partion struct {
	hashRange    HashRange
	redisManager *RedisManager
}

type PartionList []*Partion

func (me PartionList) Len() int {
	return len(me)
}

func (me PartionList) Less(i, j int) bool {
	return me[i].hashRange.Start < me[i].hashRange.End
}

func (me PartionList) Swap(i, j int) {
	me[i], me[j] = me[j], me[i]
}

// 根据hash值找到对象
func (me PartionList) Search(h int64) int {
	// TODO 后续优化为二分法查找 至少目前分区数不会很多（<10） 二分查找并不一定比遍历效率高
	for i, p := range me {
		if p.hashRange.Has(h) {
			return i
		}
	}
	return -1
}

type PartionConfig struct {
	HashRange   HashRange
	RedisConfig RedisConfig
}

func (me PartionConfig) String() string {
	sb := &strings.Builder{}
	json.NewEncoder(sb).Encode(&me)
	return sb.String()
}

type PartionManager struct {
	pl PartionList
}

func LoadPartionManager(cl []PartionConfig) (*PartionManager, error) {
	var pl []*Partion
	var err error
	defer func() {
		if err != nil {
			for _, p := range pl {
				closeClient(p.redisManager.clientList)
			}
		}
	}()

	for _, c := range cl {
		r, e := NewRedisManager(c.RedisConfig)
		if e != nil {
			err = e
			return nil, err
		}
		pl = append(pl, &Partion{hashRange: c.HashRange, redisManager: r})
	}
	return &PartionManager{pl: PartionList(pl)}, nil
}

func (this *PartionManager) getPartion(pctx ctx.PartionContext) (*RedisManager, error) {
	// TODO 将来这里还可以判断特定的Origin 给一个特定的分区（大于 max_hash）并且数据库里配置对应信息，这样超级VIP有超级VIP的服务
	h := int64(ctx.HashToUint32(pctx.Key) % ctx.MaxHash)
	idx := this.pl.Search(h)
	if idx < 0 {
		logger.Warn("[redis_partion]", pctx.String(), "not found")
		return nil, ErrPartionNotFound
	}
	return this.pl[idx].redisManager, nil
}

func (this *PartionManager) RedisDel(pctx ctx.PartionContext, keys ...string) error {
	r, err := this.getPartion(pctx)
	if err != nil {
		return err
	}
	var pkeys []string
	for _, k := range keys {
		pkeys = append(pkeys, pctx.Key+key_mid_fix+k)
	}
	return r.RedisDel(pkeys...)
}

func (this *PartionManager) RedisSetEx(pctx ctx.PartionContext, key string, dur time.Duration, value string) error {
	r, err := this.getPartion(pctx)
	if err != nil {
		return err
	}
	key = pctx.Key + key_mid_fix + key
	return r.RedisSetEx(key, dur, value)
}

func (this *PartionManager) RedisGet(pctx ctx.PartionContext, key string) (string, error) {
	r, err := this.getPartion(pctx)
	if err != nil {
		return "", err
	}
	key = pctx.Key + key_mid_fix + key
	return r.RedisGet(key)
}

func (this *PartionManager) RedisSet(pctx ctx.PartionContext, key string, value string) error {
	r, err := this.getPartion(pctx)
	if err != nil {
		return err
	}
	key = pctx.Key + key_mid_fix + key
	return r.RedisSet(key, value)
}

func (this *PartionManager) RedisHDel(pctx ctx.PartionContext, key string, fields ...string) (int64, error) {
	r, err := this.getPartion(pctx)
	if err != nil {
		return 0, err
	}
	key = pctx.Key + key_mid_fix + key
	return r.RedisHDel(key, fields...)
}

func (this *PartionManager) RedisExpire(pctx ctx.PartionContext, key string, expiration time.Duration) (bool, error) {
	r, err := this.getPartion(pctx)
	if err != nil {
		return false, err
	}
	key = pctx.Key + key_mid_fix + key
	return r.RedisExpire(key, expiration)
}

func (this *PartionManager) RedisHSet(pctx ctx.PartionContext, key, filed string, value interface{}) (bool, error) {
	r, err := this.getPartion(pctx)
	if err != nil {
		return false, err
	}
	key = pctx.Key + key_mid_fix + key
	return r.RedisHSet(key, filed, value)
}

func (this *PartionManager) RedisHGetAll(pctx ctx.PartionContext, key string) (map[string]string, error) {
	r, err := this.getPartion(pctx)
	if err != nil {
		return nil, err
	}
	key = pctx.Key + key_mid_fix + key
	return r.RedisHGetAll(key)
}
