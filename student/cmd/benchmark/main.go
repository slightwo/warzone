package main

import (
	"battleworld/protocol"
	"flag"
	"fmt"
	"math/rand"
	"net"
	"sync"
	"sync/atomic"
	"time"
)

var (
	successOps    uint64
	totalLatency  uint64
	activeClients uint32

	clientCountFlag     = flag.Int("clients", 500, "number of clients")
	durationSecondsFlag = flag.Int("duration", 60, "test duration seconds")
)

func main() {
	flag.Parse()

	var wg sync.WaitGroup

	clientCount := *clientCountFlag
	durationSeconds := *durationSecondsFlag

	fmt.Printf("启动并发吞吐量压测...\n")
	fmt.Printf("并发客户端数: %d\n", clientCount)
	fmt.Printf("测试时长: %d 秒\n", durationSeconds)

	// 控制测试结束
	stopCh := make(chan struct{})

	// 启动客户端
	for i := 0; i < clientCount; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()

			// 连接网关
			conn, err := net.Dial("tcp", protocol.GatewayAddr)
			if err != nil {
				return
			}
			pc := protocol.NewConn(conn)
			defer pc.Close()

			// 快速登录
			username := fmt.Sprintf("bench_user_%d", id)
			err = pc.Send(protocol.Message{
				Type:     protocol.TypeQuickEnter,
				Username: username,
				Password: "123",
			})
			if err != nil {
				return
			}

			// 读取 Auth 响应
			authResp, err := pc.Receive()
			if err != nil || !authResp.OK {
				return
			}

			atomic.AddUint32(&activeClients, 1)
			defer atomic.AddUint32(&activeClients, ^uint32(0))

			// 扔一个后台读 goroutine 把不断推过来的 State 消耗掉
			go func() {
				for {
					_, err := pc.Receive()
					if err != nil {
						return
					}
				}
			}()

			// 进行压测（随机移动）
			dirs := []string{protocol.DirUp, protocol.DirDown, protocol.DirLeft, protocol.DirRight}
			for {
				select {
				case <-stopCh:
					return
				default:
					dir := dirs[rand.Intn(len(dirs))]
					start := time.Now()
					err := pc.Send(protocol.Message{
						Type: protocol.TypeMove,
						Dir:  dir,
					})

					if err == nil {
						latency := time.Since(start).Microseconds()
						atomic.AddUint64(&successOps, 1)
						atomic.AddUint64(&totalLatency, uint64(latency))
					} else {
						return // 发送失败则退出
					}
					// 控制一下发包频率，不能完全死循环否则会撑爆缓存
					time.Sleep(10 * time.Millisecond)
				}
			}
		}(i)
	}

	time.Sleep(time.Duration(durationSeconds) * time.Second)
	close(stopCh) // 停止产生新请求

	// 这里直接计算数据即可
	totalOps := atomic.LoadUint64(&successOps)
	fmt.Printf("\n=== 压测结果 ===\n")
	fmt.Printf("活跃客户端最高: %d (目标: %d)\n", atomic.LoadUint32(&activeClients), clientCount)
	if totalOps > 0 {
		qps := float64(totalOps) / float64(durationSeconds)
		avgLatency := float64(atomic.LoadUint64(&totalLatency)) / float64(totalOps) / 1000.0
		fmt.Printf("总请求数: %d\n", totalOps)
		fmt.Printf("吞吐量(QPS): %.2f req/s\n", qps)
		fmt.Printf("平均延迟: %.2f ms\n", avgLatency)
	} else {
		fmt.Println("\n未成功执行任何请求，服务器可能无法建立连接或所有测试提前报错。")
	}
}
