package log

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"sync"
)

const (
	namespace  = "kingkong"
	subsystem  = "log"
	LabelLevel = "level" // 请求的路径
)

var _once_monitor sync.Once
var MetricsException *prometheus.CounterVec // warn error fatal 日志次数

func MetricsInit() {
	_once_monitor.Do(func() {
		MetricsException = promauto.NewCounterVec(prometheus.CounterOpts{
			Namespace: namespace,
			Subsystem: subsystem,
			Name:      "exception",
			Help:      "日志数量",
		}, []string{LabelLevel})
	})
}

func MetricsExceptionInc(l string) {
	if MetricsException != nil {
		MetricsException.WithLabelValues(l).Inc()
	}
}

func init() {
	MetricsInit() // 自动初始化监控
	for l := WARN; l <= FATAL; l++ {
		// MetricsExceptionInc(l.String())
	}
}
