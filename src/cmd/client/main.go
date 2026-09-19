package main

import (
	"bufio"
	"fmt"
	"os"
	"sync/atomic"
	"time"

	"battleworld/pb"
)

func main() {
	reader := bufio.NewReader(os.Stdin)
	addr := chooseGateway(reader)

	var stream *gameClient
	var state = (*pb.WorldState)(nil)
	var err error
	for {
		mode, username, password, confirm := chooseAuth(reader)
		stream, state, err = auth(addr, mode, username, password, confirm)
		if err == nil {
			break
		}
		fmt.Printf("%s进入失败：%v%s\n", colorRed, err, colorReset)
		time.Sleep(700 * time.Millisecond)
	}
	defer stream.Close()

	restoreTTY, err := enterRawMode()
	if err != nil {
		fmt.Fprintf(os.Stderr, "切换终端即时输入模式失败：%v\n", err)
		os.Exit(1)
	}
	defer restoreTTY()

	fmt.Print("\033[?1049h\033[2J\033[H\033[?25l")
	defer fmt.Print("\033[?25h\033[?1049l")

	setState(state)
	drawUI()

	done := make(chan struct{})
	go receiveGameUpdates(stream, done)

	var nextRequestID atomic.Uint64
	nextRequestID.Store(1)
	keyReader := bufio.NewReader(os.Stdin)
	for {
		select {
		case <-done:
			return
		default:
		}

		key, err := keyReader.ReadByte()
		if err != nil {
			return
		}
		if key == 3 {
			_ = sendPlayerCommand(stream, &nextRequestID, logoutCommand())
			return
		}

		command, note, ok := commandForKey(key, isShopOpen())
		if !ok {
			if key == 'p' || key == 'P' {
				setShopOpen(true)
				drawUI()
			} else if key == 'r' || key == 'R' {
				addClientNote("已刷新界面")
				drawUI()
			} else if key == 'q' || key == 'Q' {
				_ = sendPlayerCommand(stream, &nextRequestID, logoutCommand())
				return
			}
			continue
		}
		if command == nil {
			setShopOpen(false)
			drawUI()
			continue
		}

		addClientNote(note)
		if err := sendPlayerCommand(stream, &nextRequestID, command); err != nil {
			addClientNote("指令发送失败：" + err.Error())
			drawUI()
			return
		}
		drawUI()
	}
}
