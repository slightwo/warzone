package main

import (
	"bufio"
	"fmt"
	"net"
	"strings"
	"time"

	"battleworld/protocol"
)

func chooseGateway(reader *bufio.Reader) string {
	for {
		fmt.Print("\033[2J\033[H")
		fmt.Println(renderBanner("战场入口", "1. 单机测试  2. 连接指定网关"))
		fmt.Print(colorGold + "[1/2] 请选择连接方式：" + colorReset)
		choice := readLine(reader)

		switch choice {
		case "1", "":
			return protocol.GatewayAddr
		case "2":
			fmt.Print(colorCyan + "请输入网关 IP：" + colorReset)
			host := readLine(reader)
			if host == "" {
				host = "127.0.0.1"
			}
			fmt.Print(colorCyan + "请输入网关端口：" + colorReset)
			port := readLine(reader)
			if port == "" {
				port = "9310"
			}
			return net.JoinHostPort(host, port)
		default:
			fmt.Println(colorRed + "无效选择，请重新输入。" + colorReset)
			time.Sleep(700 * time.Millisecond)
		}
	}
}

func chooseAuth(reader *bufio.Reader) (string, string, string, string) {
	for {
		fmt.Print("\033[2J\033[H")
		fmt.Println(renderBanner("身份验证", "1. 登录  2. 注册"))
		fmt.Print(colorGold + "[1/2] 请选择：" + colorReset)
		choice := readLine(reader)
		mode := authModeLogin
		switch choice {
		case "1", "":
			mode = authModeLogin
		case "2":
			mode = authModeRegister
		default:
			fmt.Println(colorRed + "无效选择，请重新输入。" + colorReset)
			time.Sleep(700 * time.Millisecond)
			continue
		}

		fmt.Print(colorCyan + "用户名：" + colorReset)
		username := readLine(reader)
		fmt.Print(colorCyan + "密码：" + colorReset)
		password := readLine(reader)
		confirm := ""
		if mode == authModeRegister {
			fmt.Print(colorCyan + "确认密码：" + colorReset)
			confirm = readLine(reader)
			if password != confirm {
				fmt.Println(colorRed + "两次输入的密码不一致，请重新输入。" + colorReset)
				time.Sleep(900 * time.Millisecond)
				continue
			}
		}
		return mode, username, password, confirm
	}
}

func readLine(reader *bufio.Reader) string {
	text, _ := reader.ReadString('\n')
	return strings.TrimSpace(text)
}
