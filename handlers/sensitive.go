package handlers

import (
	"bufio"
	"fmt"
	"github.com/gin-gonic/gin"
	"github.com/importcjj/sensitive"
	"io/ioutil"
	"net/http"
	"net/url"
	"os"
	dify "sensitiveService/dify-go-sdk"
	"sensitiveService/utils/log"
	"sensitiveService/utils/redis"
	"strings"
	"time"
)

const (
	redis_key_prefix = "dify_session_"
)

var (
	filter                       = sensitive.New()
	redisMng *redis.RedisManager = nil
)

func SetRedis(r *redis.RedisManager) {
	redisMng = r
}

// LoadSensitiveWords 从文件加载敏感词
func LoadSensitiveWords(filename string) {
	file, err := os.Open(filename)
	if err != nil {
		panic(err)
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		filter.AddWord(scanner.Text())
	}
	if err := scanner.Err(); err != nil {
		panic(err)
	}
}

// AddSensitiveWord 添加敏感词的 HTTP 处理函数
func AddSensitiveWord(c *gin.Context) {
	var json struct {
		Word string `json:"word"`
	}

	if err := c.ShouldBindJSON(&json); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	filter.AddWord(json.Word)
	c.JSON(http.StatusOK, gin.H{"status": "word added"})
}

// CheckText 检查文本是否包含敏感词的 HTTP 处理函数
func CheckText(c *gin.Context) {
	var json struct {
		Text string `json:"text"`
	}

	if err := c.ShouldBindJSON(&json); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	if o, word := filter.FindIn(json.Text); o {
		c.JSON(http.StatusOK, gin.H{"contains_sensitive_word": true, "word": word})
	} else {
		c.JSON(http.StatusOK, gin.H{"contains_sensitive_word": false})
	}
}

type PoeClientReq struct {
	UserMsg        string `json:"user_msg"`
	UserWechatId   string `json:"user_wechat_id"`
	UserWechatName string `json:"user_wechat_name"`
	AiWechatId     string `json:"ai_wechat_id"`
	AiWechatName   string `json:"ai_wechat_name"`
	SeqId          int64  `json:"seq_id"`
	TimeStamp      int64  `json:"time_stamp"`
	DifyKey        string `json:"dify_key"`
}

func DifyCompletionMessages(client *dify.DifyClient, msg, cid string) (string, string) {
	payload, err := dify.PrepareCompletionPayload(map[string]interface{}{})
	if err != nil {
		log.Fatal("failed to prepare payload: %v\n", err.Error())
		return "", ""
	}

	// normal response
	completionMessagesResponse, err := client.ChatMessages(payload, msg, cid, nil)
	if err != nil {
		log.Fatal("failed to get completion messages:", err.Error())
		return "", ""
	}
	fmt.Println(completionMessagesResponse)
	fmt.Println()

	return completionMessagesResponse.ConversationID, completionMessagesResponse.Answer
}

func DifyReq(clientReq *PoeClientReq) string {
	client, err := dify.CreateDifyClient(dify.DifyClientConfig{Key: clientReq.DifyKey, Host: "https://api.dify.ai/v1", User: clientReq.UserWechatId})
	if err != nil {
		log.Fatal("failed to create DifyClient:", err.Error())
		return ""
	}

	redis_key := redis_key_prefix + clientReq.DifyKey + "-" + clientReq.UserWechatId
	cid, err := redisMng.RedisGet(redis_key)
	if err != nil || len(cid) < 1 {
		cid = ""
	}
	log.Info("redis find cid: UserWechatId", clientReq.UserWechatId, " DifyKey:", clientReq.DifyKey, " , cid:", cid)

	retCid, answer := DifyCompletionMessages(client, clientReq.UserMsg, cid)
	if len(cid) < 1 && len(retCid) > 1 {
		redisMng.RedisSet(redis_key, retCid)
	}

	return answer
}

func PoeReq(clientReq *PoeClientReq) string {
	t := time.Now().UnixMilli()
	uri := fmt.Sprintf("user_msg=%s&user_wechat_id=%s&user_wechat_name=%s&ai_wechat_id=%s&ai_wechat_name=%s&seq_id=%d&time_stamp=%d", clientReq.UserMsg, clientReq.UserWechatId, clientReq.UserWechatName, clientReq.AiWechatId, clientReq.AiWechatName, t, t/1000)
	escapeUrl := url.QueryEscape(uri)
	poe_url := "https://daily-super-elf.ngrok-free.app/from_user_msg?" + escapeUrl
	method := "POST"

	client := &http.Client{}
	req, err := http.NewRequest(method, poe_url, nil)

	if err != nil {
		fmt.Println(err)
		return ""
	}

	req.Header.Add("User-Agent", "Apifox/1.0.0 (https://apifox.com)")

	res, err := client.Do(req)
	if err != nil {
		fmt.Println(err)
		return ""
	}
	defer res.Body.Close()

	body, err := ioutil.ReadAll(res.Body)
	if err != nil {
		fmt.Println(err)
		return ""
	}
	return string(body)
}

// CheckText 检查文本是否包含敏感词的 HTTP 处理函数
func AIChat(c *gin.Context) {

	req := PoeClientReq{}
	if err := c.ShouldBindJSON(&req); err != nil {
		log.Info("err:", err.Error())
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	//优先使用 poe
	res := PoeReq(&req)
	log.Info("PoeReq resp:", res)

	if len(res) < 2 {
		log.Info("err: len(res) < 2")
		c.JSON(http.StatusBadRequest, gin.H{"error": "no message"})
	}
	if strings.Index(res, "doctype html") >= 1 {
		log.Info("poe return doctype html")
		res = ""
	}

	if len(res) < 1 {
		res = DifyReq(&req)
	}

	c.JSON(http.StatusOK, gin.H{"msg": res})

}
