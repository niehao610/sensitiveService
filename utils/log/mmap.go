package log

import (
	"github.com/edsrzf/mmap-go"
	"io/ioutil"
	"os"
	"sync"
	"syscall"
	"time"
)

var (
	defaultTmpLogPagesize = syscall.Getpagesize() * 10
	cacheCount            = 100
)

type Mmap struct {
	data     mmap.MMap     // 与文件映射的内存
	b        []byte        // 用于flush
	dataC    chan string   // 用于写入的通道
	stopC    chan struct{} // 停止写入
	stop     bool
	f        *os.File // 日志文件
	FilePath string   // 文件路径
	current  int      // 在什么位置写
	size     int      // 与文件映射的大小
	m        *sync.Mutex
}

func NewMmap(filePath string, size int) (*Mmap, error) {
	// 文件映射的大小必须是页数的倍数，如果不是，则自动根据大小调整为相应倍数
	if size%syscall.Getpagesize() != 0 {
		size = (size / syscall.Getpagesize()) * syscall.Getpagesize()
	}
	if size == 0 {
		size = syscall.Getpagesize() * 10
	}

	// 构建对应的结构体，以配后续使用
	m := &Mmap{
		size:     size,
		FilePath: filePath,
		dataC:    make(chan string, cacheCount),
		stopC:    make(chan struct{}, 1),
		stop:     false,
		m:        new(sync.Mutex),
		b:        make([]byte, 0),
	}

	// 使用channel方式，同步写入
	go m.wait()
	//go m.rename()
	return m, m.init(filePath)
}

// 初始化log信息
func (m *Mmap) init(filePath string) error {
	err := m.flushData(filePath)
	if err != nil {
		return err
	}
	err = m.setFileInfo(filePath)
	if err != nil {
		return err
	}

	err = m.allocate()
	if err != nil {
		return err
	}
	return nil
}

// MMAP映射
func (m *Mmap) allocate() error {
	if m.f == nil {
		m.setFileInfo(m.FilePath)
	}
	defer func() {
		m.f.Close()
		m.f = nil
	}()

	// MMAP映射时，文件必须有相应大小的内容，即需要相应大小的占位符
	if _, err := m.f.WriteAt(make([]byte, m.size), int64(m.current)); nil != err {
		return err
	}

	// 映射
	//data, err := syscall.Mmap(int(m.f.Fd()), 0, m.size, syscall.PROT_WRITE|syscall.PROT_READ, syscall.MAP_SHARED)
	data, err := mmap.Map(m.f, mmap.RDWR, 0)
	//data, err := unix.Mmap(int(m.f.Fd()), 0, m.size, unix.PROT_WRITE, unix.MAP_SHARED)
	if nil != err {
		return err
	}
	m.data = data
	return nil
}

// 设置映射的文件
func (m *Mmap) setFileInfo(filePath string) error {
	// 打开文件
	f, err := os.OpenFile(filePath, os.O_RDWR|os.O_CREATE, 0644)
	if nil != err {
		return err
	}

	m.f = f
	return nil
}

// 关闭所有
func (m *Mmap) Close() error {
	m.stopC <- struct{}{}
	// 需要时间去处理后续操作，包括未写入的数据
	time.Sleep(100 * time.Millisecond)
	return nil
}

// 关闭文件映射
func (m *Mmap) unmap() error {
	// 关闭映射
	if err := m.data.Unmap(); nil != err {
		return err
	}

	// 将未写入的内容清空
	// 如果未清空，在文件末位未写入位置，将会出现大量占位符
	//err := os.Truncate(m.FilePath, int64(m.current))
	//if err != nil {
	//	return err
	//}
	return nil
}

// 接收写入内容
func (m *Mmap) Write(content []byte) error {
	if !m.stop {
		m.dataC <- string(content)
	}
	return nil
}

// 等待内容写入
func (m *Mmap) wait() {
	t := time.NewTimer(time.Second)
	for {
		select {
		case content, ok := <-m.dataC:
			// 通道被关闭且服务停止，则关闭映射
			if !ok && m.stop {
				m.unmap()
				return
			}
			if len(content) == 0 {
				return
			}
			//剩余空间不足 先写入日志文件再缓存
			if m.current+len(content) > m.size/2 {
				m.flush()
			}
			m.write([]byte(content))
		case <-t.C:
			//写入日志文件
			m.flush()
			t.Reset(time.Second)

		case <-m.stopC:
			// 停止往channel里继续写数据
			m.stop = true
			// 关闭channel
			close(m.dataC)
		}
	}
}

// 写入数据到data，并修改current
func (m *Mmap) write(content []byte) {
	m.m.Lock()
	defer m.m.Unlock()
	//为了避免panic，如果content长度大于初始化的文件大小，则进行日志截取
	// 内容写入文件
	for i, v := range content {
		if m.current+i >= m.size {
			break
		}
		m.data[m.current+i] = v
	}
	m.current += len(content)
}

// 将data数据flush到logfile
func (m *Mmap) flush() {
	m.m.Lock()
	defer m.m.Unlock()

	m.b = m.b[:0]
	for _, b := range m.data {
		//todo 判断是否到结束了
		if b == 0 {
			break
		}
		m.b = append(m.b, b)
	}

	if len(m.b) > 0 {
		logObj.logfile.Write(m.b)
	}
	for i, b := range m.data {
		//todo 判断是否到结束了
		if b == 0 {
			break
		}
		m.data[i] = 0
	}
	m.current = 0
}

// 将文件中的数据flush到logfile
func (m *Mmap) flushData(filePath string) error {
	if _, err := os.Stat(filePath); err != nil {
		return nil
	}
	data, err := ioutil.ReadFile(filePath)
	if err != nil {
		return err
	}

	m.b = m.b[:0]
	for _, b := range data {
		//todo 判断是否到结束了
		if b == 0 {
			break
		}
		m.b = append(m.b, b)
	}
	if len(m.b) > 0 {
		logObj.logfile.Write(m.b)
	}
	return nil
}
