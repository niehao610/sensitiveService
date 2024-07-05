package main

import (
	"github.com/gin-gonic/gin"
	"sensitiveService/handlers"
)

func main() {
	router := gin.Default()

	// 加载模板文件
	router.LoadHTMLGlob("templates/*")

	// 加载敏感词文件
	handlers.LoadSensitiveWords("sensitive_words.txt")

	// 设置路由
	router.GET("/", func(c *gin.Context) {
		c.HTML(200, "index.html", nil)
	})

	// 设置路由
	router.POST("/add_word", handlers.AddSensitiveWord)
	router.POST("/check_text", handlers.CheckText)

	// 启动服务器
	router.Run(":8085")
}
