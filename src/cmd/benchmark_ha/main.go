package main

import (
	"battleworld/protocol"
	"fmt"
	"math/rand"
	"net"
	"sync"
	"sync/atomic"
	"time"
)

func main() {
	var wg sync.WaitGroup
	var activeClients uint32
	var totalRTO int64
	var maxRTO int64
	var recoveredCount uint32

	clientCount := 100 // 我们只需要100个客户端就能测出 RTO
	totalDuration := 20 * time.Second

	fmt.Println("=== 高可用 (HA) 与故障恢复 (RTO) 测试 ===")
	fmt.Printf("并发模拟客户端: %d\n", clientCount)
	fmt.Println("测试流程: 加入绿区 -> 持续拉取状态 -> 外部触发宕机指令 -> 记录从发生错误到重连/恢复状态的毫秒数(RTO)。")
	fmt.Println("-------------------------------------------")

	stopCh := make(chan struct{})

	// 启动客户端
	for i := 0; i < clientCount; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()

			conn, err := net.Dial("tcp", protocol.GatewayAddr)
			if err != nil {
				return
			}
			pc := protocol.NewConn(conn)
			defer pc.Close()

			username := fmt.Sprintf("ha_user_%d", id)
			err = pc.Send(protocol.Message{
				Type:     protocol.TypeQuickEnter,
				Username: username,
				Password: "123",
			})
			if err != nil {
				return
			}

			authResp, err := pc.Receive()
			if err != nil || !authResp.OK {
				return
			}

			atomic.AddUint32(&activeClients, 1)

			// 状态机记录
			var downStart time.Time
			var inDownState bool
			var originalNode string

			// 持续接收服务器的状态广播或者恢复后的响应
			go func() {
				for {
					msg, err := pc.Receive()
					if err != nil || msg.Type == protocol.TypeLogout {
						return // 真实的掉线
					}

					if msg.Type == protocol.TypeError {
						// 服务端返回了错误（例如移动请求被拒绝），说明节点不可用或正在切换
						if !inDownState {
							downStart = time.Now()
							inDownState = true
						}
					} else if msg.Type == protocol.TypeState || msg.OK {
						if msg.State != nil {
							currentNode := msg.State.Map.NodeID
							if originalNode == "" {
								originalNode = currentNode
							} else if originalNode == "node-a" && currentNode != "node-a" && !inDownState {
								// 节点发生了静默切换（客户端没有收到报错，直接被系统重路由了）
								atomic.AddUint32(&recoveredCount, 1)
								originalNode = currentNode
							}
						}

						// 如果之前处于错误状态，现在收到了正常包，说明切换完毕！
						if inDownState {
							rto := time.Since(downStart).Milliseconds()
							atomic.AddInt64(&totalRTO, rto)
							atomic.AddUint32(&recoveredCount, 1)

							for {
								currentMax := atomic.LoadInt64(&maxRTO)
								if rto <= currentMax || atomic.CompareAndSwapInt64(&maxRTO, currentMax, rto) {
									break
								}
							}
							inDownState = false
						}
					}
				}
			}()

			// 进行移动指令压测，以探测服务连通性
			dirs := []string{protocol.DirUp, protocol.DirDown, protocol.DirLeft, protocol.DirRight}
			for {
				select {
				case <-stopCh:
					return
				default:
					dir := dirs[rand.Intn(len(dirs))]
					_ = pc.Send(protocol.Message{
						Type: protocol.TypeMove,
						Dir:  dir,
					})
					time.Sleep(100 * time.Millisecond) // 每100ms发送一次，足以极速感知网络分区
				}
			}
		}(i)
	}

	// 充当指挥官，等待稳定后注入故障
	go func() {
		time.Sleep(5 * time.Second)
		fmt.Printf("[T=5s] 混沌工程指令: 调用 Admin 接口击杀 node-a 节点！💥\n")
		// 使用跟 cmd/admin 工具一样的发送规范
		conn, err := net.Dial("tcp", protocol.GatewayAddr)
		if err != nil {
			return
		}
		admin := protocol.NewConn(conn)
		admin.Send(protocol.Message{Type: protocol.TypeAdmin, Action: "fail", NodeID: "node-a"})
		admin.Close()
	}()

	time.Sleep(totalDuration)
	close(stopCh) // 停止产生新请求
	time.Sleep(1 * time.Second)

	fmt.Println("\n=== 高可用容灾 (RTO) 测试报告 ===")
	fmt.Printf("成功建立长连接的客户端: %d\n", atomic.LoadUint32(&activeClients))

	rc := atomic.LoadUint32(&recoveredCount)
	fmt.Printf("记录到断点并完成主备恢复的客户端条数: %d\n", rc)
	if rc > 0 {
		avgRTO := float64(atomic.LoadInt64(&totalRTO)) / float64(rc)
		fmt.Printf("网络平均故障恢复时间 (Avg RTO): %.2f 毫秒\n", avgRTO)
		fmt.Printf("网络最长故障恢复时间 (Max RTO): %d 毫秒\n", atomic.LoadInt64(&maxRTO))
	} else {
		fmt.Println("未检测到有效容灾切换，请检查单点故障注入逻辑。")
	}
}
