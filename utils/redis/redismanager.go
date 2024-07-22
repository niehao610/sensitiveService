package redis

import (
	"fmt"
	"github.com/go-redis/redis"
	"github.com/pkg/errors"
	"reflect"
	"runtime"
	logger "sensitiveService/utils/log"
	"strings"
	"sync/atomic"
	"time"
)

const Nil = redis.Nil

/**
github.com/go-redis/redis
该组件自带连接池，自动重连，从连接池取连接超时，连接超时，发送超时，读超时。
兼容性保证：
1. 请保证所有接口都使用基本类型,返回也都是基本类型。
2. 请保证向外只暴露函数和常量，不暴露变量。
**/

type REDIS_TYPE string

func (me REDIS_TYPE) Valid() bool {
	return REDIS_TYPE_COMM == me || REDIS_TYPE_SENTINEL == me || REDIS_TYPE_CLUSSTER == me
}

const (
	log_prefix = "[redis]"

	REDIS_TYPE_COMM     = REDIS_TYPE("comm")
	REDIS_TYPE_SENTINEL = REDIS_TYPE("sentinel")
	REDIS_TYPE_CLUSSTER = REDIS_TYPE("cluster")
)

//var redis_attrid_access int = 1031
//var redis_attrid_access_Fail int = 1032

/****
为了增加特性不再动调整接口，这里将初始化需要的参考汇总
*****/

type RedisConfig struct {
	Type       REDIS_TYPE
	AddrList   string
	Passwd     string
	DB         int
	MasterName string
	PoolSize   int // 默认为  runtime.NumCPU() * 5
}

func (this *RedisConfig) Valid() error {
	if !this.Type.Valid() {
		return fmt.Errorf("invalid type", this.Type)
	}

	if REDIS_TYPE_SENTINEL == this.Type && this.MasterName == "" {
		return fmt.Errorf("sentinel type but master name empty")
	}

	if strings.TrimSpace(this.AddrList) == "" {
		return fmt.Errorf("invalid addrlist", this.AddrList)
	}

	return nil
}

func (this *RedisConfig) String() string {
	return fmt.Sprintf(
		"type:%s addrs:%s passwd:%s db:%d master_name:%s poolsize:%d",
		this.Type,
		this.AddrList,
		this.Passwd,
		this.DB,
		this.MasterName,
		this.PoolSize)
}

// 预研功能自动向flag注册，省代码。
func (this *RedisConfig) pre_autoRegistrToFlag(prefix string) {
	//	&this.Type = flag.Int(fmt.Sprintf("%s_type"), 0, "")
	//	&this.AddrList = flag.String(fmt.Sprintf("%s_type"), "", "")
}

type RedisManager struct {
	clientList []redis.Cmdable
	size       uint32
	cur        uint32
	isCluster  bool
}

func NewRedisManager(config RedisConfig) (*RedisManager, error) {
	if config.Type == "" {
		config.Type = REDIS_TYPE_COMM
	}
	config.AddrList = strings.TrimSpace(config.AddrList)
	if config.PoolSize == 0 {
		config.PoolSize = runtime.NumCPU() * 5
	}

	if err := config.Valid(); err != nil {
		logger.Warn(log_prefix, "invalid config:", config.String(), "err:", err.Error())
		return nil, err
	}

	logger.All(log_prefix, "init config:", config.String())

	switch config.Type {
	case REDIS_TYPE_COMM:
		return newCommManager(config)
	case REDIS_TYPE_SENTINEL:
		return newSentinelManager(config)
	case REDIS_TYPE_CLUSSTER:
		return newClusterManager(config)
	default:
		logger.Warn("BUG:", log_prefix, "invalid redis type.", config.String())
		return nil, fmt.Errorf("bug: unknown redis type")
	}
}

func newClusterManager(config RedisConfig) (*RedisManager, error) {
	addrs := strings.Split(config.AddrList, ",")
	c := redis.NewClusterClient(&redis.ClusterOptions{
		Addrs:              addrs,
		Password:           config.Passwd,
		PoolSize:           config.PoolSize, // cup个数*5 并发数
		DialTimeout:        5 * time.Second, // 5秒
		ReadTimeout:        5 * time.Minute, // 5分钟
		WriteTimeout:       5 * time.Second, // 5秒
		PoolTimeout:        6 * time.Minute, // 5分钟 比ReadTimeout要大一点
		IdleTimeout:        200 * time.Second,
		IdleCheckFrequency: 20 * time.Second,
	})

	cmd := c.Ping()
	if cmd.Err() != nil {
		logger.Error(log_prefix, "connect to redis faild! config:", config.String(), "err:", cmd.Err().Error())
		return nil, cmd.Err()
	}

	logger.Info("init cluster redis manager success. config:", config.String())
	return &RedisManager{clientList: []redis.Cmdable{c}, size: 1, cur: 0, isCluster: true}, nil
}

func newSentinelManager(config RedisConfig) (*RedisManager, error) {
	addrs := strings.Split(config.AddrList, ",")
	c := redis.NewFailoverClient(&redis.FailoverOptions{
		MasterName:         config.MasterName,
		SentinelAddrs:      addrs,
		Password:           config.Passwd,
		DB:                 config.DB,
		PoolSize:           config.PoolSize, // cup个数*5 并发数
		DialTimeout:        5 * time.Second, // 5秒
		ReadTimeout:        5 * time.Minute, // 5分钟
		WriteTimeout:       5 * time.Second, // 5秒
		PoolTimeout:        6 * time.Minute, // 5分钟 比ReadTimeout要大一点
		IdleTimeout:        200 * time.Second,
		IdleCheckFrequency: 20 * time.Second,
	})

	cmd := c.Ping()
	if cmd.Err() != nil {
		logger.Error(log_prefix, "connect to redis faild! config:", config.String(), "err:", cmd.Err().Error())
		return nil, cmd.Err()
	}

	logger.Info("init sentinel redis manager success. config:", config.String())
	return &RedisManager{clientList: []redis.Cmdable{c}, size: 1, cur: 0}, nil
}

func newCommManager(config RedisConfig) (*RedisManager, error) {
	var err error = nil
	var clientList []redis.Cmdable
	defer func() {
		if err != nil {
			closeClient(clientList)
		}
	}()

	addrs := strings.Split(config.AddrList, ",")

	for _, addr := range addrs {
		c := redis.NewClient(&redis.Options{
			Addr:               addr,
			Password:           config.Passwd,
			DB:                 config.DB,
			PoolSize:           config.PoolSize, // cup个数*5 并发数
			DialTimeout:        5 * time.Second, // 5秒
			ReadTimeout:        5 * time.Minute, // 5分钟
			WriteTimeout:       5 * time.Second, // 5秒
			PoolTimeout:        6 * time.Minute, // 5分钟 比ReadTimeout要大一点
			IdleTimeout:        200 * time.Second,
			IdleCheckFrequency: 20 * time.Second,
		})
		err := c.Ping().Err()
		if err == nil {
			logger.All(log_prefix, "connect to redis succ! config:", config.String())
			clientList = append(clientList, c)
		} else {
			logger.Error(log_prefix, "connect to redis faild! config:", config.String(), "err:", err.Error())
			return nil, err
		}
	}

	size := len(clientList)
	if size <= 0 {
		return nil, fmt.Errorf("none available connection")
	}

	logger.All("init comm redis manager success. config:", config.String())
	return &RedisManager{clientList: clientList, size: uint32(size), cur: 8}, nil
}

func closeClient(clientList []redis.Cmdable) {
	if clientList != nil && len(clientList) > 0 {
		for _, client := range clientList {
			if c, ok := client.(*redis.Client); ok {
				c.Close()
				continue
			}
			if c, ok := client.(*redis.ClusterClient); ok {
				c.Close()
				continue
			}
			logger.Warn(log_prefix, "close redis fail. client: ", client, " type:", reflect.TypeOf(client))
		}
	}
}

func (this *RedisManager) getAClient() redis.Cmdable {
	id := atomic.AddUint32(&this.cur, 1)
	return this.clientList[id%this.size]
}

func (this *RedisManager) RedisSet(key string, value string) error {
	rclient := this.getAClient()
	err := rclient.Set(key, value, 0).Err()
	if err != nil {
		logger.Error(log_prefix, "set", key, value, "err:", err)
		return err
	}
	// logger.All(log_prefix, "set", key, value, "success.")
	return nil
}

func (this *RedisManager) RedisDel(key ...string) error {
	rclient := this.getAClient()
	if this.isCluster {
		for _, k := range key {
			err := rclient.Del(k).Err()
			if err != nil && err != redis.Nil {
				logger.Error(log_prefix, "del", key, "err:", err)
			} else {
				logger.All(log_prefix, "del", key, "success.")
			}
		}
	} else {
		err := rclient.Del(key...).Err()
		if err != nil && err != redis.Nil {
			logger.Error(log_prefix, "del", key, "err:", err)
			return err
		} else {
			logger.All(log_prefix, "del", key, "success.")
		}
	}

	return nil
}

func (this *RedisManager) RedisSetEx(key string, dur time.Duration, value string) error {
	rclient := this.getAClient()
	err := rclient.Set(key, value, dur).Err()
	if err != nil {
		logger.Warn(log_prefix, "set key:", key, "value:", value, "dur:", dur, "err:", err.Error())
		return err
	}
	return nil
}

func (this *RedisManager) RedisGet(key string) (string, error) {
	rclient := this.getAClient()
	v, err := rclient.Get(key).Result()
	if err != nil && err != redis.Nil {
		logger.Error(log_prefix, "redis get ", key, " err :", err)
		return "", err
	}

	if err == redis.Nil {
		return "", nil
	}

	return v, nil
}

func (this *RedisManager) RedisIncrBy(key string, step int64) (int64, error) {
	rclient := this.getAClient()
	v, err := rclient.IncrBy(key, step).Result()
	if err != nil {
		logger.Error(log_prefix, "redis.IncrBy", key, step, "err:", err)
		return v, err
	}
	return v, err
}

func (this *RedisManager) RedisRPop(key string) (v string, err error) {
	rclient := this.getAClient()
	v, err = rclient.RPop(key).Result()

	if err != nil {
		if err == redis.Nil {
			// 没数据
			v = ""
			err = nil
		} else {
			logger.Warn(log_prefix, "rpop", key, "err:", err.Error())
			return "", err
		}
	}

	return v, err
}

func (this *RedisManager) BLPop(queue string) (data string, err error) {
	rclient := this.getAClient()
	temp, err := rclient.BRPop(0, queue).Result()
	if err != nil {
		logger.Warn(log_prefix, "blpop", queue, "err:", err.Error())
		return "", err
	}
	return temp[1], nil
}

// redis list没有批量删除并获取，现用事务的方式获取
func (this *RedisManager) PipelineRedisLRangAndLTrim(key string, size int64) (v []string, err error) {
	rclient := this.getAClient()
	pipeline := rclient.Pipeline()
	pipeline.LRange(key, 0, size-1)
	pipeline.LTrim(key, size, -1)
	cmdArr, err := pipeline.Exec()
	if err != nil && err != redis.Nil {
		logger.Warn(log_prefix, "pipelineExec", key, "err:", err.Error())
	}
	stringSliceCmd := cmdArr[0].(*redis.StringSliceCmd)
	v = stringSliceCmd.Val()
	return v, err
}

func (this *RedisManager) RedisLPush(key string, val ...string) (int64, error) {
	rclient := this.getAClient()
	valArr := make([]interface{}, 0)
	for _, v := range val {
		valArr = append(valArr, v)
	}
	cmd := rclient.LPush(key, valArr...)
	if cmd.Err() != nil {
		logger.Warn(log_prefix, "lpush", key, val, "err:", cmd.Err().Error())
	}
	return cmd.Result()
}

func (this *RedisManager) RedisLPushArr(key string, val []interface{}) (int64, error) {
	rclient := this.getAClient()
	cmd := rclient.LPush(key, val...)
	if cmd.Err() != nil {
		logger.Warn(log_prefix, "lpush", key, val, "err:", cmd.Err().Error())
	}
	return cmd.Result()
}

func (this *RedisManager) RedisLLen(key string) (int64, error) {
	rclient := this.getAClient()
	cmd := rclient.LLen(key)
	if cmd.Err() != nil {
		logger.Warn(log_prefix, "llen", key, "err:", cmd.Err().Error())
		return -1, cmd.Err()
	}
	return cmd.Val(), nil

}

func (this *RedisManager) RedisSMembers(key string) ([]string, error) {
	rclient := this.getAClient()
	v, err := rclient.SMembers(key).Result()
	if err != nil && err != redis.Nil {
		logger.Error("smembers ", key, "return:", err)
		return nil, err
	}
	return v, nil
}

func (this *RedisManager) RedisSpop(key string) (string, error) {
	rclient := this.getAClient()
	v, err := rclient.SPop(key).Result()
	if err != nil && err != redis.Nil {
		logger.Error("spop ", key, "return:", err)
		return "", err
	}
	return v, nil
}

func (this *RedisManager) RedisSIsMembers(key string, val interface{}) (bool, error) {
	rclient := this.getAClient()
	v, err := rclient.SIsMember(key, val).Result()
	if err != nil && err != redis.Nil {
		logger.Warn(log_prefix, "SIsMember", key, "return:", err)
		return false, err
	}
	if err == redis.Nil {
		return false, nil
	}
	return v, nil
}

func (this *RedisManager) RedisSAddInt64(key string, valList ...int64) error {
	rclient := this.getAClient()
	objs := make([]interface{}, len(valList))
	for i, v := range valList {
		objs[i] = v
	}

	rst := rclient.SAdd(key, objs...)
	if rst != nil && rst.Err() != nil {
		logger.Warn(log_prefix, "sadd", key, valList, "err:", rst.Err().Error())
		return rst.Err()
	}
	return nil
}

func (this *RedisManager) RedisSAdd(key string, valList ...string) error {
	rclient := this.getAClient()
	objs := make([]interface{}, len(valList))
	for i, v := range valList {
		objs[i] = v
	}

	rst := rclient.SAdd(key, objs...)
	if rst != nil && rst.Err() != nil {
		logger.Warn(log_prefix, "sadd", key, valList, "err:", rst.Err().Error())
		return rst.Err()
	}
	return nil
}

func (this *RedisManager) RedisSDel(key string, valList ...string) error {
	rclient := this.getAClient()
	objs := make([]interface{}, len(valList))
	for i, v := range valList {
		objs[i] = v
	}
	cmd := rclient.SRem(key, objs...)
	if cmd.Err() != nil && cmd.Err() != redis.Nil {
		logger.Warn(log_prefix, "sdel", key, valList, "err:", cmd.Err().Error())
		return cmd.Err()
	}
	return nil
}

func (this *RedisManager) RedisSCard(key string) (int64, error) {
	rclient := this.getAClient()
	cmd := rclient.SCard(key)
	if cmd.Err() != nil && cmd.Err() != redis.Nil {
		logger.Warn(log_prefix, "scard", key, "err:", cmd.Err())
	}
	return cmd.Result()
}

// 没有记录返回空和nil
func (this *RedisManager) RedisSPop(key string) (string, error) {
	rclient := this.getAClient()
	data, err := rclient.SPop(key).Result()
	if err != nil {
		if err == redis.Nil {
			return "", nil
		}
		logger.Warn(log_prefix, "scard", key, "err:", err.Error())
		return "", err
	}
	return data, nil
}

func (this *RedisManager) PipelineSCard(keys []string) []int64 {
	rclient := this.getAClient()
	pipeline := rclient.Pipeline()
	keysnum := 0
	for _, key := range keys {
		if len(key) > 0 {
			logger.Debug(log_prefix, "pipeline scard key ", key)
			pipeline.SCard(key)
			keysnum++
		}
	}

	cmds, err := pipeline.Exec()

	logger.Debug(log_prefix, "pipe result:", cmds, err)

	var result []int64

	if err != nil && err != redis.Nil {
		logger.Warn(log_prefix, "redisClient pipeline err %s", err.Error())
	} else {
		logger.Debug(log_prefix, "redisClient scard pipeline ok")

		for _, cmd := range cmds {
			logger.Debug(log_prefix, cmd, "ret result:", cmd.(*redis.IntCmd).Val())
			if cmd.(*redis.IntCmd).Err() != nil {
				logger.Error(cmd.(*redis.IntCmd).Err())
			} else {
				result = append(result, cmd.(*redis.IntCmd).Val())
			}
		}
	}
	return result
}

func (this *RedisManager) Keys(key string) []string {
	rclient := this.getAClient()
	cmd := rclient.Keys(key)
	if cmd.Err() != nil && cmd.Err() != redis.Nil {
		logger.Warn(log_prefix, "keys", key, "err:", cmd.Err().Error())
		return nil
	}

	return cmd.Val()
}

func (this *RedisManager) Scan(cursor uint64, match string, count int64) ([]string, uint64, error) {
	rclient := this.getAClient()
	cmd := rclient.Scan(cursor, match, count)
	if cmd.Err() != nil && cmd.Err() != redis.Nil {
		logger.Warn(log_prefix, "scan cursor", cursor, "err:", cmd.Err().Error())
	}

	return cmd.Result()
}

func (this *RedisManager) Type(key string) string {
	rclient := this.getAClient()
	cmd := rclient.Type(key)
	if cmd.Err() != nil && cmd.Err() != redis.Nil {
		logger.Warn(log_prefix, "get type err:", cmd.Err().Error())
		return ""
	}

	return cmd.Val()
}

func (this *RedisManager) Eval(script string, keys []string, args []interface{}) (err error) {
	rclient := this.getAClient()
	cmd := rclient.Eval(script, keys, args...)
	if cmd != nil {
		err = cmd.Err()
		if err != nil {
			if err == redis.Nil {
				err = nil
			} else {
				logger.Warn(log_prefix, "eval", script, keys, args, "err:", err.Error())
			}
		}
	}
	return
}

func (this *RedisManager) RedisHincrby(key, field string, incr int64) (int64, error) {
	rclient := this.getAClient()
	cmd := rclient.HIncrBy(key, field, incr)
	if cmd == nil {
		return 0, nil
	}
	err := cmd.Err()
	if err == redis.Nil {
		return 0, nil
	}

	if err != nil {
		logger.Warn(log_prefix, "HIncrBy", key, field, incr, "err:", err.Error())
		return 0, err
	}
	return cmd.Result()
}

func (this *RedisManager) RedisHincrby2(key, field string, incr int64) (ire int64, err error) {
	rclient := this.getAClient()
	cmd := rclient.HIncrBy(key, field, incr)
	if cmd != nil {
		err = cmd.Err()
		if err != nil {
			if err == redis.Nil {
				err = nil
			} else {
				logger.Warn(log_prefix, "HIncrBy", key, field, incr, "err:", err.Error())
			}
		}
	}
	return cmd.Result()
}

func (this *RedisManager) PipelineRedisHincrby(key string, fields []string, incr int64) (err error) {
	rclient := this.getAClient()
	pipeline := rclient.Pipeline()
	for _, field := range fields {
		if len(field) > 0 {
			logger.Debug(log_prefix, "pipeline hincrby key ", key)
			pipeline.HIncrBy(key, field, incr)
		}
	}

	cmds, err := pipeline.Exec()

	logger.Debug(log_prefix, "pipe result:", cmds, err)

	if err != nil && err != redis.Nil {
		logger.Warn(log_prefix, "redisClient pipeline err %s", err.Error())
		return err
	} else {
		logger.Debug(log_prefix, "redisClient hincrby pipeline ok")
	}

	return nil
}

func (this *RedisManager) RedisHExists(key, field string) (bool, error) {
	rclient := this.getAClient()
	cmd := rclient.HExists(key, field)
	if cmd.Err() != nil {
		logger.Warn(log_prefix, "hexists", key, field, "err:", cmd.Err().Error())
	}
	return cmd.Result()
}

func (this *RedisManager) RedisHGet(key, field string) (string, error) {
	rclient := this.getAClient()
	cmd := rclient.HGet(key, field)
	if cmd != nil {
		if cmd.Err() == nil {
			return cmd.Result()
		} else {
			if cmd.Err() == redis.Nil {
				return "", nil
			} else {
				logger.Warn(log_prefix, "HGet", key, field, "err:", cmd.Err().Error())
				return "", cmd.Err()
			}
		}
	} else {
		return "", nil
	}
}

func (this *RedisManager) RedisHDel(key string, fields ...string) (int64, error) {
	rclient := this.getAClient()
	cmd := rclient.HDel(key, fields...)
	if cmd.Err() != nil {
		logger.Warn(log_prefix, "hdel", key, fields, "err:", cmd.Err().Error())
	}
	return cmd.Result()
}

/*
************************************************************************************
 zhj extend
************************************************************************************
*/

// Redis `SET key value [expiration]` command.
// Use expiration for `SETEX`-like behavior.
// Zero expiration means the key has no expiration time.
func (this *RedisManager) RedisSet2(key string, value interface{}, expiration time.Duration) (string, error) {
	v, err := this.getAClient().Set(key, value, expiration).Result()
	if err != nil {
	}
	return v, err
}

// Redis `SET key value [expiration] NX` command.
//
// Zero expiration means the key has no expiration time.
func (this *RedisManager) RedisSetNX(key string, value interface{}, expiration time.Duration) (bool, error) {
	v, err := this.getAClient().SetNX(key, value, expiration).Result()
	if err != nil {
	}
	return v, err
}

func (this *RedisManager) RedisHMSet(key string, val map[string]interface{}) (string, error) {
	v, err := this.getAClient().HMSet(key, val).Result()
	if err != nil {
	}
	return v, err
}

func (this *RedisManager) RedisHMGet(key string, fileds ...string) ([]interface{}, error) {
	v, err := this.getAClient().HMGet(key, fileds...).Result()
	if err != nil && err != redis.Nil {
		return nil, err
	}
	if err == redis.Nil {
		return nil, nil
	}
	return v, err
}

func (this *RedisManager) RedisHSet(key, field string, val interface{} /*成对出现filed:value*/) (bool, error) {
	v, err := this.getAClient().HSet(key, field, val).Result()
	if err != nil {
	}
	return v, err
}

// Z represents sorted set member.
//
//	type Z struct {
//		Score  float64
//		Member interface{}
//	}
type Z = redis.Z

func (this *RedisManager) RedisZAdd(key string, zlist ...Z) (int64, error) {
	v, err := this.getAClient().ZAdd(key, zlist...).Result()
	if err != nil {
	}
	return v, err
}

func (this *RedisManager) RedisZRem(key string, members ...interface{}) (int64, error) {
	v, err := this.getAClient().ZRem(key, members...).Result()
	if err != nil {
	}
	return v, err
}

func (this *RedisManager) RedisZRemRangeByScore(key string, min, max string) (int64, error) {
	v, err := this.getAClient().ZRemRangeByScore(key, min, max).Result()
	if err != nil {
	}
	return v, err
}

func (this *RedisManager) RedisExpire(key string, expiration time.Duration) (bool, error) {
	v, err := this.getAClient().Expire(key, expiration).Result()
	if err != nil {
	}
	return v, err
}

func (this *RedisManager) RedisExpireAt(key string, tm time.Time) (bool, error) {
	v, err := this.getAClient().ExpireAt(key, tm).Result()
	if err != nil {
	}
	return v, err
}

// Redis Zrevrangebyscore 返回有序集中指定分数区间内的所有的成员。有序集成员按分数值递减(从大到小)的次序排列。
// 具有相同分数值的成员按字典序的逆序(reverse lexicographical order )排列。
// 除了成员按分数值递减的次序排列这一点外， ZREVRANGEBYSCORE 命令的其他方面和 ZRANGEBYSCORE 命令一样。
// 区间的取值使用闭区间
// ZREVRANGEBYSCORE key max min [WITHSCORES] [LIMIT offset count]
// 最小 -inf 最大 +inf
// 指定区间内，带有分数值(可选)的有序集成员的列表。
func (this *RedisManager) RedisZRevRangeByScore(key, min, max string, offset, num int64) ([]redis.Z, error) {
	opt := redis.ZRangeBy{
		Min:    min,
		Max:    max,
		Offset: offset,
		Count:  num,
	}
	v, err := this.getAClient().ZRevRangeByScoreWithScores(key, opt).Result()
	if err != nil && err != redis.Nil {
		return nil, err
	}

	if err == redis.Nil {
		return nil, nil
	}

	return v, err
}

// Redis Zrangebyscore 返回有序集合中指定分数区间的成员列表。有序集成员按分数值递增(从小到大)次序排列。
// 具有相同分数值的成员按字典序来排列(该属性是有序集提供的，不需要额外的计算)。
// 默认情况下，区间的取值使用闭区间 (小于等于或大于等于)，你也可以通过给参数前增加 ( 符号来使用可选的开区间 (小于或大于)。
func (this *RedisManager) RedisZRangeByScore(key, min, max string, offset, num int64) ([]redis.Z, error) {
	opt := redis.ZRangeBy{
		Min:    min,
		Max:    max,
		Offset: offset,
		Count:  num,
	}
	v, err := this.getAClient().ZRangeByScoreWithScores(key, opt).Result()
	if err != nil && err != redis.Nil {
		return nil, err
	}
	if err == redis.Nil {
		return nil, nil
	}
	return v, err
}

// Redis Zcount 命令用于计算有序集合中指定分数区间的成员数量;
// 命令基本语法：ZCOUNT key min max;
// 最小 -inf 最大 +inf
// 返回值:分数值在 min 和 max 之间的成员的数量。如果key不存在返回-1
func (this *RedisManager) RedisZCount(key, min, max string) (int64, error) {
	v, err := this.getAClient().ZCount(key, min, max).Result()
	if err != nil && err != redis.Nil {
		return -1, err
	}

	if err == redis.Nil {
		return 0, nil
	}

	return v, err
}

// ZCard 返回有序集的成员个数
func (this *RedisManager) RedisZCard(key string) (int64, error) {
	v, err := this.getAClient().ZCard(key).Result()
	if err != nil {
		return 0, err
	}
	return v, err
}

// ZRevRange
// 返回有序集key中，指定区间内的成员。其中成员的位置按score值递减(从高到低)来排列。
// 具有相同score值的成员按字典序的反序排列。 除了成员排序相反外，ZREVRANGE命令的其他方面和ZRANGE命令一样。
// 返回值 数组: 指定范围的元素列表
func (this *RedisManager) RedisZRevRange(key string, start, stop int64) ([]string, error) {
	v, err := this.getAClient().ZRevRange(key, start, stop).Result()
	if err != nil && err != redis.Nil {
		return nil, err
	}
	if err == redis.Nil {
		return nil, nil
	}
	return v, err
}

// ZScore 返回有序集 key 中，成员 member 的 score 值 O(1)
func (this *RedisManager) RedisZScore(key, member string) (float64, bool, error) {
	v, err := this.getAClient().ZScore(key, member).Result()
	if err != nil && err != redis.Nil {
		return 0, false, err
	}

	if err == redis.Nil {
		return 0, false, nil
	}
	return v, true, nil
}

// ZRevRangeWithScores
// 返回有序集key中，指定区间内的成员。其中成员的位置按score值递减(从高到低)来排列。
// 具有相同score值的成员按字典序的反序排列。 除了成员排序相反外，ZREVRANGE命令的其他方面和ZRANGE命令一样。
// 返回值 数组: 指定范围的元素列表(含有分数)。
func (this *RedisManager) RedisZRevRangeWithScores(key string, start, stop int64) ([]redis.Z, error) {
	v, err := this.getAClient().ZRevRangeWithScores(key, start, stop).Result()
	if err != nil && err != redis.Nil {
		return nil, err
	}
	if err == redis.Nil {
		return nil, nil
	}
	return v, err
}

func (this *RedisManager) RedisExists(keys ...string) (int64, error) {
	client := this.getAClient()

	if this.isCluster {
		total := int64(0)
		for _, k := range keys {
			v, err := client.Exists(k).Result()
			if err != nil && err != redis.Nil {
				return 0, err
			} else {
				total += v
			}
		}
		return total, nil
	} else {
		v, err := client.Exists(keys...).Result()
		if err != nil {
		}
		return v, err
	}

}

func (this *RedisManager) RedisGeoDist(key, member1, member2, unit string) (float64, error) {
	v, err := this.getAClient().GeoDist(key, member1, member2, unit).Result()
	if err != nil {
	}
	return v, err
}

func (this *RedisManager) TTL(key string) (time.Duration, error) {
	return this.getAClient().TTL(key).Result()
}

// GEOADD key longitude latitude member [longitude latitude member …]
func (this *RedisManager) RedisGeoAdd(key string, longitude, latitude float64, name string) (int64, error) {
	v, err := this.getAClient().GeoAdd(key, &redis.GeoLocation{
		Name:      name,
		Longitude: longitude,
		Latitude:  latitude,
	}).Result()
	if err != nil {
	}
	return v, err
}

func (this *RedisManager) RedisGeoRadius(key string, longitude, latitude float64, query *redis.GeoRadiusQuery) ([]redis.GeoLocation, error) {
	v, err := this.getAClient().GeoRadius(key, longitude, latitude, query).Result()
	if err != nil {
	}
	return v, err
}

func (this *RedisManager) RedisHGetAll(key string) (map[string]string, error) {
	v, err := this.getAClient().HGetAll(key).Result()
	if err != nil && err != redis.Nil {
		logger.Error("redis hgetall ", key, "err:", err)
		fieldMap := map[string]string{}
		return fieldMap, err
	}

	if err == redis.Nil {
		return v, nil
	}
	return v, err
}

func (this *RedisManager) RedisIsMembers2(key, val string) (bool, error) {
	rclient := this.getAClient()
	v, err := rclient.SIsMember(key, val).Result()
	if err != nil {
	}
	return v, err
}

/***********************************************************************************/
func (this *RedisManager) SRem(key string, members ...interface{}) (int64, error) {
	v, err := this.getAClient().SRem(key, members...).Result()
	if err != nil {
	}
	return v, err
}

func (this *RedisManager) LRange(key string, start, stop int64) ([]string, error) {
	v, err := this.getAClient().LRange(key, start, stop).Result()
	if err != nil {
	}
	return v, err
}

func (this *RedisManager) LIndex(key string, index int64) (data string, err error) {
	rclient := this.getAClient()
	temp, err := rclient.LIndex(key, index).Result()
	if err != nil {
		logger.Warn(log_prefix, "LIndex", key, "err:", err.Error())
		return "", err
	}
	return temp, nil
}

func (this *RedisManager) LRem(key string, count int64, value interface{}) (ire int64, err error) {
	rclient := this.getAClient()
	temp, err := rclient.LRem(key, count, value).Result()
	if err != nil {
		logger.Warn(log_prefix, "LRem", key, count, value, "err:", err.Error())
		return 0, err
	}
	return temp, nil
}

func (this *RedisManager) RedisLPush2(key string, vals ...interface{}) (int64, error) {
	rclient := this.getAClient()
	cmd := rclient.LPush(key, vals...)
	if cmd.Err() != nil {
		logger.Warn(log_prefix, "lpush", key, vals, "err:", cmd.Err().Error())
	}
	return cmd.Result()
}

/*zhj**********************************************************************************/

// pipline
func (this *RedisManager) Pipeline_Get() redis.Pipeliner {
	rclient := this.getAClient()
	pipeline := rclient.Pipeline()
	return pipeline
}

func (this *RedisManager) Pipeline_CmdAdd(pip redis.Pipeliner, cmd string, val []string) (err error) {
	defer func() {
		if err != nil {
			logger.Debug(log_prefix, "pipe add cmd err:", err.Error())
		}
	}()

	switch cmd {
	case "get":
		if len(val) < 1 {
			err = errors.New("not enough parameters")
			return
		}
		pip.Get(val[0])
	case "set":
		if len(val) < 2 {
			err = errors.New("not enough parameters")
			return
		}
		pip.Set(val[0], val[1], 0)
	case "sadd":
		if len(val) < 2 {
			err = errors.New("not enough parameters")
			return
		}
		pip.SAdd(val[0], val[1])
	case "smembers":
		if len(val) < 1 {
			err = errors.New("not enough parameters")
			return
		}
		pip.SMembers(val[0])
	case "spop":
		if len(val) < 1 {
			err = errors.New("not enough parameters")
			return
		}
		pip.SPop(val[0])
	default:
		err = errors.New("not support cmd")
		return
	}
	return nil
}

// 成功的key []string, 未成功的key []string,错误提示 error
func (this *RedisManager) Pipeline_Exec(pip redis.Pipeliner) ([]string, []string, error) {

	cmds, err := pip.Exec()

	//logger.Debug(log_prefix, "pipe result:", cmds, err)

	var succ []string
	var fail []string

	if err != nil && err != redis.Nil {
		logger.Error(log_prefix, "[import] redisClient pipeline err %s", err.Error())
		return nil, nil, err
	} else {
		logger.Debug(log_prefix, "redisClient exec pipeline ok")

		for _, cmd := range cmds {
			if cmd.Err() != nil {
				//logger.Error("process error",cmd.String())
				fail = append(fail, cmd.Name())
			} else {
				succ = append(succ, cmd.Name())
			}
		}
		return succ, fail, nil
	}
}

//end pipline

func (this *RedisManager) RedisPipelineSetBit(key string, offsets []int64, value int) ([]int64, error) {
	rclient := this.getAClient()
	pipeline := rclient.Pipeline()
	for _, offset := range offsets {
		//logger.Debug(log_prefix, "pipeline scard key ", key)
		pipeline.SetBit(key, offset, value)
	}

	cmds, err := pipeline.Exec()
	var result []int64

	if err != nil {
		return result, err
	}

	//logger.Debug(log_prefix, "pipe result:", cmds, err)

	if err != nil && err != redis.Nil {
		logger.Warn(log_prefix, "redisClient pipeline err %s", err.Error())
	} else {
		//logger.Debug(log_prefix, "redisClient scard pipeline ok")

		for _, cmd := range cmds {
			//logger.Debug(log_prefix, cmd, "ret result:", cmd.(*redis.IntCmd).Val())
			if cmd.(*redis.IntCmd).Err() != nil {
				result = append(result, cmd.(*redis.IntCmd).Val())
				logger.Error(cmd.(*redis.IntCmd).Err())
			}
		}
	}
	return result, nil
}
