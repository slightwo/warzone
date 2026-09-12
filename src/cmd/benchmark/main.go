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

const benchmarkProtocol = "gateway"

var (
	users       int
	duration    int
	gatewayAddr string
)

func init() {
	flag.IntVar(&users, "u", 100, "number of concurrent users")
	flag.IntVar(&users, "users", 100, "number of concurrent users")
	flag.IntVar(&duration, "t", 30, "test duration in seconds")
	flag.IntVar(&duration, "time", 30, "test duration in seconds")
	flag.StringVar(&gatewayAddr, "addr", protocol.GatewayAddr, "Gateway address")
}

type benchmarkMetrics struct {
	commandConfirmed atomic.Int64
	commandRejected  atomic.Int64
	transportErrors  atomic.Int64
	stateUpdates     atomic.Int64

	mu        sync.Mutex
	latencies []time.Duration
}

type pendingCommands struct {
	mu      sync.Mutex
	started map[uint64]time.Time
}

func newPendingCommands() *pendingCommands {
	return &pendingCommands{started: make(map[uint64]time.Time)}
}

func (p *pendingCommands) start(requestID uint64, startedAt time.Time) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.started[requestID] = startedAt
}

func (p *pendingCommands) resolve(requestID uint64) (time.Time, bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	startedAt, ok := p.started[requestID]
	delete(p.started, requestID)
	return startedAt, ok
}

func (p *pendingCommands) forget(requestID uint64) {
	p.mu.Lock()
	defer p.mu.Unlock()
	delete(p.started, requestID)
}

func (m *benchmarkMetrics) addLatency(latency time.Duration) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.latencies = append(m.latencies, latency)
}

func (m *benchmarkMetrics) snapshotLatencies() []time.Duration {
	m.mu.Lock()
	defer m.mu.Unlock()
	return append([]time.Duration(nil), m.latencies...)
}

var (
	metrics      benchmarkMetrics
	peakUsers    atomic.Int64
	currentUsers atomic.Int64
)

var maps = []string{"green", "cave", "ruins"}
var dirs = []pb.Direction{
	pb.Direction_DIRECTION_UP,
	pb.Direction_DIRECTION_DOWN,
	pb.Direction_DIRECTION_LEFT,
	pb.Direction_DIRECTION_RIGHT,
}

func main() {
	flag.Parse()

	log.Printf("Starting benchmark with %d users for %d seconds against %s\n", users, duration, gatewayAddr)

	var wg sync.WaitGroup
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(duration)*time.Second)
	defer cancel()

	startTime := time.Now()
	for i := 0; i < users; i++ {
		wg.Add(1)
		go runUser(ctx, &wg, i)
		time.Sleep(5 * time.Millisecond)
	}
	wg.Wait()
	printMetrics(time.Since(startTime))
}

func runUser(ctx context.Context, wg *sync.WaitGroup, id int) {
	defer wg.Done()
	runGatewayUser(ctx, id)
}

func runGatewayUser(ctx context.Context, id int) {
	conn, err := grpc.NewClient(gatewayAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		metrics.transportErrors.Add(1)
		return
	}
	defer conn.Close()

	stream, err := pb.NewGatewayServiceClient(conn).GameStream(ctx)
	if err != nil {
		log.Printf("[user %d] open stream: %v", id, err)
		metrics.transportErrors.Add(1)
		return
	}

	username := fmt.Sprintf("bench_user_%d_%d", id, time.Now().UnixNano())
	const authRequestID = 1
	if err := stream.Send(&pb.ClientEnvelope{
		RequestId: authRequestID,
		Payload: &pb.ClientEnvelope_QuickEnter{QuickEnter: &pb.QuickEnterRequest{
			Username: username,
			Password: "password",
		}},
	}); err != nil {
		metrics.transportErrors.Add(1)
		return
	}

	auth, err := stream.Recv()
	if err != nil {
		metrics.transportErrors.Add(1)
		return
	}
	if auth.GetAuthenticated() == nil || auth.GetAuthenticated().GetState() == nil || auth.GetRequestId() != authRequestID {
		metrics.commandRejected.Add(1)
		return
	}

	currentUsers.Add(1)
	updatePeak()
	defer currentUsers.Add(-1)
	defer stream.CloseSend()

	pending := newPendingCommands()
	receiveDone := make(chan struct{})
	go func() {
		defer close(receiveDone)
		for {
			response, err := stream.Recv()
			if err != nil {
				return
			}
			if response.GetState() != nil {
				metrics.stateUpdates.Add(1)
				continue
			}
			if response.GetCommandResult() != nil {
				startedAt, ok := pending.resolve(response.GetRequestId())
				if ok {
					metrics.commandConfirmed.Add(1)
					metrics.addLatency(time.Since(startedAt))
				}
				continue
			}
			if response.GetError() != nil {
				_, pendingCommand := pending.resolve(response.GetRequestId())
				if pendingCommand || response.GetRequestId() != 0 {
					metrics.commandRejected.Add(1)
				}
			}
		}
	}()

	requestID := uint64(1)
	send := func(request *pb.ClientEnvelope) error {
		pending.start(request.GetRequestId(), time.Now())
		if err := stream.Send(request); err != nil {
			pending.forget(request.GetRequestId())
			return err
		}
		return nil
	}

	requestID++
	if err := send(&pb.ClientEnvelope{
		RequestId: requestID,
		Payload:   &pb.ClientEnvelope_SwitchMap{SwitchMap: &pb.SwitchMapCommand{MapId: maps[id%len(maps)]}},
	}); err != nil {
		metrics.transportErrors.Add(1)
		return
	}

	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-receiveDone:
			metrics.transportErrors.Add(1)
			return
		case <-ticker.C:
			requestID++
			if err := send(&pb.ClientEnvelope{
				RequestId: requestID,
				Payload:   &pb.ClientEnvelope_Move{Move: &pb.MoveCommand{Direction: dirs[rand.Intn(len(dirs))]}},
			}); err != nil {
				metrics.transportErrors.Add(1)
				return
			}
		}
	}
}

func updatePeak() {
	for {
		current := currentUsers.Load()
		peak := peakUsers.Load()
		if current <= peak || peakUsers.CompareAndSwap(peak, current) {
			return
		}
	}
}

func printMetrics(actualDuration time.Duration) {
	latencies := metrics.snapshotLatencies()
	sort.Slice(latencies, func(i, j int) bool { return latencies[i] < latencies[j] })

	confirmed := metrics.commandConfirmed.Load()
	rejected := metrics.commandRejected.Load()
	transportErrors := metrics.transportErrors.Load()
	stateUpdates := metrics.stateUpdates.Load()
	total := confirmed + rejected + transportErrors

	fmt.Println("\n========== 压测结果 ==========")
	fmt.Printf("协议版本:       %s\n", benchmarkProtocol)
	fmt.Printf("并发用户数预设: %d\n", users)
	fmt.Printf("峰值在线玩家数: %d\n", peakUsers.Load())
	fmt.Printf("实际测试时间:   %v\n", actualDuration)
	fmt.Printf("命令确认数:     %d\n", confirmed)
	fmt.Printf("业务拒绝数:     %d\n", rejected)
	fmt.Printf("传输错误数:     %d\n", transportErrors)
	fmt.Printf("状态推送数:     %d\n", stateUpdates)

	if len(latencies) > 0 {
		var totalLatency time.Duration
		for _, latency := range latencies {
			totalLatency += latency
		}
		percentile := func(percent float64) time.Duration {
			return latencies[int(float64(len(latencies)-1)*percent)]
		}
		fmt.Printf("命令确认平均 RTT: %v\n", totalLatency/time.Duration(len(latencies)))
		fmt.Printf("命令确认 P50 RTT:  %v\n", percentile(0.50))
		fmt.Printf("命令确认 P95 RTT:  %v\n", percentile(0.95))
		fmt.Printf("命令确认 P99 RTT:  %v\n", percentile(0.99))
	} else {
		fmt.Println("没有收到任何命令确认 RTT 记录")
	}
	if actualDuration > 0 {
		fmt.Printf("确认吞吐量(QPS): %.2f req/s\n", float64(confirmed)/actualDuration.Seconds())
	}
	if total > 0 {
		fmt.Printf("总错误率:       %.2f%%\n", float64(rejected+transportErrors)/float64(total)*100)
	}
	fmt.Println("=============================")
}
