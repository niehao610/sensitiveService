package main

import (
	"fmt"
	"github.com/gin-gonic/gin"
	"os"
	"sensitiveService/handlers"
	"sensitiveService/utils"
	logger "sensitiveService/utils/log"
	"sensitiveService/utils/redis"
)

const (
	default_log_dir  = "/tmp"
	default_log_name = "woocomm.log"
)

var g_conf *utils.GlobalConf

// 初始化配置
func initConfig(confpath string) {
	conf, err := utils.NewWithOptions(&utils.Options{Filename: confpath})
	if err != nil {
		fmt.Fprintln(os.Stderr, err.Error())
		os.Exit(1) // 与其错误运行不如直接退出
	}
	conf.ParseAll()
	g_conf = conf
}

// 获取配置
func ConfigGetString(key string) (string, bool) {
	return g_conf.GetDict().GetString("", key)
}

// 获取配置
func ConfigGetDefaultString(key string, defaultVal string) string {
	val, ok := g_conf.GetDict().GetString("", key)
	if ok {
		return val
	} else {
		return defaultVal
	}
}

func ConfigGetInt(key string) (int, bool) {
	return g_conf.GetDict().GetInt("", key)
}

func ConfigGetBool(key string) (bool, bool) {
	return g_conf.GetDict().GetBool("", key)
}

func ConfigGetDefaultBool(key string, defaultVal bool) bool {
	val, ok := g_conf.GetDict().GetBool("", key)
	if ok {
		return val
	} else {
		return defaultVal
	}
}

func ConfigDumpToString() string {
	return g_conf.GetDict().String()
}

func ConfigGetKeys() []string {
	keys := []string{}

	for k, _ := range *g_conf.GetDict() {
		keys = append(keys, k)
	}
	return keys
}

// 设置日志级别,默认配置如下:
// log_console=false # 是否输出到命令行
// log_level=2 # 日志级别
// log_dir=/tmp # 日志目录
func initLogger() {
	//var log_console = false
	if b, ok := ConfigGetBool("log_console"); ok && b {
		//log_console = b
		logger.SetConsole(b)
	} else {
		logger.SetConsole(false)
	}
	if i, ok := ConfigGetInt("log_level"); ok {
		logger.SetLevel(logger.Level(i))
	} else {
		logger.SetLevel(logger.INFO)
	}

	log_dir := default_log_dir
	if s, ok := ConfigGetString("log_dir"); ok {
		log_dir = s
	}

	log_name := default_log_name
	if s, ok := ConfigGetString("log_name"); ok {
		log_name = s
	}

	tmp_log_name := ""
	if s, ok := ConfigGetString("log_mmap_name"); ok {
		tmp_log_name = s
	}
	logger.LoggerSetRollingDaily(log_dir, log_name, tmp_log_name)
}

func main() {

	confpath := "svr.conf"
	if len(os.Args) <= 1 {
	} else {
		confpath = os.Args[1]
	}

	if confpath == "-help" || confpath == "-h" {
		fmt.Println("./sensitiveService svr.conf")
		return
	}

	initConfig(confpath)
	initLogger()

	redis_addrlist, _ := ConfigGetString("redis_addrlist")
	redis_passwd, _ := ConfigGetString("redis_passwd")
	redis_db, _ := ConfigGetInt("redis_db")

	redisCfg := redis.RedisConfig{
		Type:       "comm",
		AddrList:   redis_addrlist,
		Passwd:     redis_passwd,
		DB:         redis_db,
		MasterName: "",
	}
	redisMng, err := redis.NewRedisManager(redisCfg)

	if err != nil {
		fmt.Println("init redis err:" + err.Error())
		return
	}

	router := gin.Default()

	// 加载模板文件
	router.LoadHTMLGlob("templates/*")

	// 加载敏感词文件
	handlers.LoadSensitiveWords("sensitive_words.txt")
	handlers.SetRedis(redisMng)

	// 设置路由
	router.GET("/", func(c *gin.Context) {
		c.HTML(200, "index.html", nil)
	})

	// 设置路由
	router.POST("/add_word", handlers.AddSensitiveWord)
	router.POST("/check_text", handlers.CheckText)
	router.POST("/from_user_msg", handlers.AIChat)

	// 启动服务器
	router.Run(":8086")
}
