package redis

import (
	"fmt"
)

//global key name format config
const (
	//string
	//type string
	//key		[uid]
	//value  	[appkey]
	KEY_UID_APPKEY_FORMAT = "uid_appkey:%v"
)

func KeyUidAppkey(uid uint64) string {
	return fmt.Sprintf(KEY_UID_APPKEY_FORMAT, uid)
}

// ****** begin rate limit push stat redis key ******
// zhj
// 2020.6.1

// key1 一条消息基本信息
// hash
// key rlt_msginfo_{msgid}
// field
// info value [info json] 内容proto.Marshal(OnePushTaskGW)
// rate value [速率 个/s]
// total_{pt('i'/'a'/'b'/'c')} value [各平台目标数]
// total value [目标总量]
// done value [已经处理的目标]
// itime value [消息生成时间点，时间戳s]
// lasttime value [最近处理时间点，时间戳s]
// t3push_enable value [是否是支持厂商推送]
// android_broadcast value [广播时，是否支持厂商全量推，比如vivo全量推接口]
// interval value [时间间隔，如果总目标数大于或等于总时间秒数时interval=1s，当总目标数小于总时间秒数时interval=总时间秒数/总目标数]
const (
	RLT_KEY1_FIELD_TOTAL                = "total"
	RLT_KEY1_FIELD_DONE                 = "done"
	RLT_KEY1_FIELD_LASTTIME             = "lasttime"
	RLT_KEY1_FIELD_INFO                 = "info"
	RLT_KEY1_FIELD_RATE                 = "rate"
	RLT_KEY1_FIELD_ITIME                = "itime"
	RLT_KEY1_FIELD_IS_ANDROID_BROADCAST = "android_broadcast"
	RLT_KEY1_FIELD_T3PUSH_ENABLE        = "t3push_enable"
	RLT_KEY1_FIELD_INTERVAL             = "interval"
)

func RLT_KEY1(msgid interface{}) string {
	return fmt.Sprintf("rlt_msginfo_%v", msgid)
}
func RLT_KEY1_FIELD_PT_TOTAL(cpt int8) string {
	return fmt.Sprintf("total_%c", cpt)
}

// key2 一条定速消息拆分成的子任务
// set
// key rlt_tasks_{msgid}
// value gzip  nplat(pt)
// uidlist
func RLT_KEY2(msgid interface{}) string {
	return fmt.Sprintf("rlt_tasks_%v", msgid)
}

// key3 所有定速消息
// list
// key rlt_all_rate_msg
// value  {msgid}
func RLT_KEY3() string {
	return fmt.Sprintf("rlt_all_rate_push_msg")
}

// key4 定速消息分布式锁键
// string
// key rlt_processing_{msgid}
func RLT_KEY4(msgid interface{}) string {
	return fmt.Sprintf("rlt_processing_%v", msgid)
}

// ****** end rate limit push stat redis key ******
