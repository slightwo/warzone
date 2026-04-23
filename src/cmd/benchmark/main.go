package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"battleworld/pb"
	"battleworld/protocol"

	_ "net/http/pprof"
)

var (
	users    int
	duration int
)

func init() {
	flag.IntVar(&users, "u", 100, "number of concurrent users")
	flag.IntVar(&users, "users", 100, "number of concurrent users")
	flag.IntVar(&duration, "t", 30, "test duration in seconds")
	flag.IntVar(&duration, "time", 30, "test duration in seconds")
}

var (
	successCount int64
	errorCount   int64
	peakUsers    int64
	currentUsers int64
	latencies    []time.Duration
	latMu        sync.Mutex
)

var maps = []string{"green", "cave", "ruins"}
var dirs = []string{protocol.DirUp, protocol.DirDown, protocol.DirLeft, protocol.DirRight}

func main() {
	flag.Parse()

	log.Printf("Starting benchmark with %d users for %d seconds\n", users, duration)

	var wg sync.WaitGroup
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(duration)*time.Second)
	defer cancel()

	startTime := time.Now()

	for i := 0; i < users; i++ {
		wg.Add(1)
		go runUser(ctx, &wg, i)
		time.Sleep(5 * time.Millisecond) // 稍微分散登录请求，避免瞬间雪崩
	}

	wg.Wait()
	endTime := time.Now()

	printMetrics(endTime.Sub(startTime))
}

func runUser(ctx context.Context, wg *sync.WaitGroup, id int) {
	defer wg.Done()

	conn, err := grpc.NewClient(protocol.GatewayAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		atomic.AddInt64(&errorCount, 1)
		return
	}
	defer conn.Close()

	client := pb.NewGatewayServiceClient(conn)
	stream, err := client.GameStream(ctx)
	if err != nil {
		log.Printf("[User %d] Error dialing stream: %v", id, err)
		atomic.AddInt64(&errorCount, 1)
		return
	}

	// 1. 登录
	username := fmt.Sprintf("bench_user_%d_%d", id, time.Now().UnixNano())
	req := &protocol.Message{
		Type:     protocol.TypeQuickEnter,
		Username: username,
		Password: "password",
	}
	err = stream.Send(protocol.ToProtoMessage(*req))
	if err != nil {
		log.Printf("[User %d] Error sending login: %v", id, err)
		atomic.AddInt64(&errorCount, 1)
		return
	}

	// 等待认证成功
	respPb, err := stream.Recv()
	if err != nil {
		log.Printf("[User %d] Error receiving auth: %v", id, err)
		atomic.AddInt64(&errorCount, 1)
		return
	}
	resp := protocol.FromProtoMessage(respPb)
	if resp.Type == protocol.TypeError {
		log.Printf("[User %d] Server responded error: %s", id, resp.Error)
		atomic.AddInt64(&errorCount, 1)
		return
	}

	// 并发处理服务端下发的状态推送（清空接收缓冲区以避免阻塞）这也能用来计算实际RTT
	var lastReqTime time.Time
	var lastReqMu sync.Mutex

	go func() {
		for {
			rPb, err := stream.Recv()
			if err != nil {
				return
			}
			msg := protocol.FromProtoMessage(rPb)
			// 如果是状态更新，且我们刚好发起了移动请求，可以近似作为一次交互完成
			if msg.Type == protocol.TypeState {
				lastReqMu.Lock()
				if !lastReqTime.IsZero() {
					latency := time.Since(lastReqTime)
					lastReqTime = time.Time{} // reset
					lastReqMu.Unlock()

					latMu.Lock()
					latencies = append(latencies, latency)
					latMu.Unlock()
					atomic.AddInt64(&successCount, 1)
				} else {
					lastReqMu.Unlock()
				}
			} else if msg.Type == protocol.TypeError {
				atomic.AddInt64(&errorCount, 1)
			}
		}
	}()

	// 更新峰值在线数据
	atomic.AddInt64(&currentUsers, 1)
	updatePeak()
	defer atomic.AddInt64(&currentUsers, -1)

	// 2. 平均分配到不同地图
	targetMap := maps[id%len(maps)]
	err = stream.Send(protocol.ToProtoMessage(protocol.Message{
		Type:  protocol.TypeSwitchMap,
		MapID: targetMap,
	}))
	if err != nil {
		atomic.AddInt64(&errorCount, 1)
		return
	}
	time.Sleep(100 * time.Millisecond) // 等待地图切换完成

	// 3. 随机移动
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			stream.CloseSend()
			return
		case <-ticker.C:
			dir := dirs[rand.Intn(len(dirs))]
			moveReq := protocol.Message{
				Type: protocol.TypeMove,
				Dir:  dir,
			}

			lastReqMu.Lock()
			lastReqTime = time.Now()
			lastReqMu.Unlock()

			err := stream.Send(protocol.ToProtoMessage(moveReq))
			if err != nil {
				atomic.AddInt64(&errorCount, 1)
				return
			}
		}
	}
}

func updatePeak() {
	for {
		curr := atomic.LoadInt64(&currentUsers)
		p := atomic.LoadInt64(&peakUsers)
		if curr > p {
			if atomic.CompareAndSwapInt64(&peakUsers, p, curr) {
				break
			}
		} else {
			break
		}
	}
}

func printMetrics(actualDuration time.Duration) {
	fmt.Println("\n========== 压测结果 ==========")
	fmt.Printf("并发用户数预设: %d\n", users)
	fmt.Printf("峰值在线玩家数: %d\n", peakUsers)
	fmt.Printf("实际测试时间:   %v\n", actualDuration)

	if len(latencies) == 0 {
		fmt.Println("没有任何有效的请求延迟记录！是否所有链接都失败了？")
		return
	}

	// 不在运行途中排序，统一结束排序
	latMu.Lock()
	copiedLats := make([]time.Duration, len(latencies))
	copy(copiedLats, latencies)
	latMu.Unlock()

	sort.Slice(copiedLats, func(i, j int) bool {
		return copiedLats[i] < copiedLats[j]
	})

	var totalLatency time.Duration
	for _, l := range copiedLats {
		totalLatency += l
	}

	avgLatency := totalLatency / time.Duration(len(copiedLats))
	sort.Slice(copiedLats, func(i, j int) bool { return copiedLats[i] < copiedLats[j] })
	p95 := copiedLats[int(float64(len(copiedLats))*0.95)]
	p99 := copiedLats[int(float64(len(copiedLats))*0.99)]

	qps := float64(successCount) / actualDuration.Seconds()
	errRate := float64(errorCount) / float64(successCount+errorCount) * 100

	fmt.Printf("总请求数:       %d\n", successCount+errorCount)
	fmt.Printf("成功请求数:     %d\n", successCount)
	fmt.Printf("失败请求数:     %d\n", errorCount)
	fmt.Printf("平均响应时间:   %v\n", avgLatency)
	fmt.Printf("95%% 响应时间:   %v\n", p95)
	fmt.Printf("99%% 响应时间:   %v\n", p99)
	fmt.Printf("吞吐量(QPS):    %.2f req/s\n", qps)
	fmt.Printf("错误率:         %.2f%%\n", errRate)

	fmt.Println("=============================")
}
