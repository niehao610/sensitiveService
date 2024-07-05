package handlers

import (
	"bufio"
	"github.com/gin-gonic/gin"
	"github.com/importcjj/sensitive"
	"net/http"
	"os"
)

var (
	filter = sensitive.New()
)

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
