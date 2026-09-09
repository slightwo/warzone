package main

import (
	"battleworld/pb"
	"battleworld/protocol"
	"bufio"
	"context"
	"flag"
	"fmt"
	"net"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

const (
	colorReset  = "\033[0m"
	colorBold   = "\033[1m"
	colorDim    = "\033[2m"
	colorCyan   = "\033[38;5;51m"
	colorTeal   = "\033[38;5;44m"
	colorGold   = "\033[38;5;220m"
	colorOrange = "\033[38;5;208m"
	colorRed    = "\033[38;5;196m"
	colorGreen  = "\033[38;5;82m"
	colorGray   = "\033[38;5;245m"
	colorBlue   = "\033[38;5;39m"
	bgPanel     = "\033[48;5;235m"
)

const (
	clientProtocolV1 = "v1"
	clientProtocolV2 = "v2"
)

var (
	uiMu        sync.Mutex
	current     *pb.WorldState
	clientNotes []string
	shopOpen    bool
)

type gameClient interface {
	Send(*pb.ClientEnvelope) error
	Recv() (*pb.ServerEnvelope, error)
	CloseSend() error
	Close() error
}

// clientCommand constructs a fresh envelope payload for one generated request_id.
// It avoids exposing protobuf's package-private oneof interface outside pb.
type clientCommand func(uint64) *pb.ClientEnvelope

type v2GameClient struct {
	stream pb.GatewayService_GameStreamV2Client
	conn   *grpc.ClientConn
}

func (c *v2GameClient) Send(request *pb.ClientEnvelope) error {
	return c.stream.Send(request)
}

func (c *v2GameClient) Recv() (*pb.ServerEnvelope, error) {
	return c.stream.Recv()
}

func (c *v2GameClient) CloseSend() error {
	return c.stream.CloseSend()
}

func (c *v2GameClient) Close() error {
	_ = c.stream.CloseSend()
	return c.conn.Close()
}

type v1GameClient struct {
	stream pb.GatewayService_GameStreamClient
	conn   *grpc.ClientConn
}

func (c *v1GameClient) Send(request *pb.ClientEnvelope) error {
	message, err := v1MessageFromEnvelope(request)
	if err != nil {
		return err
	}
	return c.stream.Send(message)
}

func (c *v1GameClient) Recv() (*pb.ServerEnvelope, error) {
	message, err := c.stream.Recv()
	if err != nil {
		return nil, err
	}
	return v1EnvelopeFromMessage(message), nil
}

func (c *v1GameClient) CloseSend() error {
	return c.stream.CloseSend()
}

func (c *v1GameClient) Close() error {
	_ = c.stream.CloseSend()
	return c.conn.Close()
}

func main() {
	protocolVersion := flag.String("protocol-version", clientProtocolV2, "Gateway 协议版本：v2（默认）或 v1（回退）")
	flag.Parse()
	if !isSupportedClientProtocol(*protocolVersion) {
		fmt.Fprintf(os.Stderr, "不支持的协议版本 %q：仅支持 v1 或 v2\n", *protocolVersion)
		os.Exit(2)
	}

	reader := bufio.NewReader(os.Stdin)
	addr := chooseGateway(reader)

	var stream gameClient
	var state *pb.WorldState
	var err error
	for {
		mode, username, password, confirm := chooseAuth(reader)
		stream, state, err = auth(addr, *protocolVersion, mode, username, password, confirm)
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

func receiveGameUpdates(stream gameClient, done chan<- struct{}) {
	defer close(done)
	for {
		envelope, err := stream.Recv()
		if err != nil {
			addClientNote("与网关连接已断开")
			drawUI()
			return
		}
		handleServerEnvelope(envelope)
		drawUI()
	}
}

func handleServerEnvelope(envelope *pb.ServerEnvelope) {
	if envelope == nil {
		return
	}
	if authenticated := envelope.GetAuthenticated(); authenticated != nil {
		applyState(authenticated.GetState())
		return
	}
	if state := envelope.GetState(); state != nil {
		applyState(state)
		return
	}
	if result := envelope.GetCommandResult(); result != nil {
		addClientNote(fmt.Sprintf("指令 #%d 已确认：%s", envelope.GetRequestId(), result.GetMessage()))
		return
	}
	if responseError := envelope.GetError(); responseError != nil {
		addClientNote(formatGatewayError(envelope.GetRequestId(), responseError))
		return
	}
	if notice := envelope.GetNotice(); notice != nil {
		addClientNote(notice.GetMessage())
	}
}

func sendPlayerCommand(stream gameClient, nextRequestID *atomic.Uint64, command clientCommand) error {
	requestID := nextRequestID.Add(1)
	return stream.Send(command(requestID))
}

func commandForKey(key byte, shop bool) (clientCommand, string, bool) {
	if shop {
		switch key {
		case 'p', 'P', 27:
			return nil, "", true
		case '1':
			return buyItemCommand("potion"), "购买药剂", true
		case '2':
			return buyItemCommand("weapon"), "强化武器", true
		default:
			return nil, "", false
		}
	}

	switch key {
	case 'w', 'W':
		return moveCommand(pb.Direction_DIRECTION_UP), "向上移动", true
	case 's', 'S':
		return moveCommand(pb.Direction_DIRECTION_DOWN), "向下移动", true
	case 'a', 'A':
		return moveCommand(pb.Direction_DIRECTION_LEFT), "向左移动", true
	case 'd', 'D':
		return moveCommand(pb.Direction_DIRECTION_RIGHT), "向右移动", true
	case 'j', 'J', 'f', 'F':
		return attackCommand(), "发起近战攻击", true
	case 'k', 'K', 'h', 'H':
		return healCommand(), "使用药剂", true
	case 'b', 'B':
		return attackBossCommand(), "挑战世界首领", true
	case '1':
		return switchMapCommand("green"), "切换到青岚要塞", true
	case '2':
		return switchMapCommand("cave"), "切换到玄矿地窟", true
	case '3':
		return switchMapCommand("ruins"), "切换到残星遗迹", true
	default:
		return nil, "", false
	}
}

func moveCommand(direction pb.Direction) clientCommand {
	return func(requestID uint64) *pb.ClientEnvelope {
		return &pb.ClientEnvelope{RequestId: requestID, Payload: &pb.ClientEnvelope_Move{Move: &pb.MoveCommand{Direction: direction}}}
	}
}

func attackCommand() clientCommand {
	return func(requestID uint64) *pb.ClientEnvelope {
		return &pb.ClientEnvelope{RequestId: requestID, Payload: &pb.ClientEnvelope_Attack{Attack: &pb.AttackCommand{}}}
	}
}

func attackBossCommand() clientCommand {
	return func(requestID uint64) *pb.ClientEnvelope {
		return &pb.ClientEnvelope{RequestId: requestID, Payload: &pb.ClientEnvelope_AttackBoss{AttackBoss: &pb.AttackBossCommand{}}}
	}
}

func healCommand() clientCommand {
	return func(requestID uint64) *pb.ClientEnvelope {
		return &pb.ClientEnvelope{RequestId: requestID, Payload: &pb.ClientEnvelope_Heal{Heal: &pb.HealCommand{}}}
	}
}

func buyItemCommand(item string) clientCommand {
	return func(requestID uint64) *pb.ClientEnvelope {
		return &pb.ClientEnvelope{RequestId: requestID, Payload: &pb.ClientEnvelope_BuyItem{BuyItem: &pb.BuyItemCommand{Item: item}}}
	}
}

func switchMapCommand(mapID string) clientCommand {
	return func(requestID uint64) *pb.ClientEnvelope {
		return &pb.ClientEnvelope{RequestId: requestID, Payload: &pb.ClientEnvelope_SwitchMap{SwitchMap: &pb.SwitchMapCommand{MapId: mapID}}}
	}
}

func logoutCommand() clientCommand {
	return func(requestID uint64) *pb.ClientEnvelope {
		return &pb.ClientEnvelope{RequestId: requestID, Payload: &pb.ClientEnvelope_Logout{Logout: &pb.LogoutCommand{}}}
	}
}

func auth(addr, protocolVersion, mode, username, password, confirm string) (gameClient, *pb.WorldState, error) {
	if !isSupportedClientProtocol(protocolVersion) {
		return nil, nil, fmt.Errorf("不支持的协议版本 %q", protocolVersion)
	}

	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, nil, fmt.Errorf("连接网关失败：%w", err)
	}

	client := pb.NewGatewayServiceClient(conn)
	var stream gameClient
	if protocolVersion == clientProtocolV1 {
		v1Stream, err := client.GameStream(context.Background())
		if err != nil {
			_ = conn.Close()
			return nil, nil, fmt.Errorf("打开 V1 游戏流失败：%w", err)
		}
		stream = &v1GameClient{stream: v1Stream, conn: conn}
	} else {
		v2Stream, err := client.GameStreamV2(context.Background())
		if err != nil {
			_ = conn.Close()
			return nil, nil, fmt.Errorf("打开 V2 游戏流失败：%w", err)
		}
		stream = &v2GameClient{stream: v2Stream, conn: conn}
	}

	requestID := uint64(1)
	if err := stream.Send(authRequest(requestID, mode, username, password, confirm)); err != nil {
		_ = stream.Close()
		return nil, nil, fmt.Errorf("发送认证消息失败：%w", err)
	}

	reply, err := stream.Recv()
	if err != nil {
		_ = stream.Close()
		return nil, nil, fmt.Errorf("接收认证结果失败：%w", err)
	}
	if responseError := reply.GetError(); responseError != nil {
		_ = stream.Close()
		return nil, nil, fmt.Errorf("%s", formatGatewayError(reply.GetRequestId(), responseError))
	}
	authenticated := reply.GetAuthenticated()
	if authenticated == nil || authenticated.GetState() == nil {
		_ = stream.Close()
		return nil, nil, fmt.Errorf("网关返回了无效认证响应")
	}
	return stream, authenticated.GetState(), nil
}

func authRequest(requestID uint64, mode, username, password, confirm string) *pb.ClientEnvelope {
	request := &pb.ClientEnvelope{RequestId: requestID}
	switch mode {
	case protocol.TypeRegister:
		request.Payload = &pb.ClientEnvelope_Register{Register: &pb.RegisterRequest{
			Username:        username,
			Password:        password,
			ConfirmPassword: confirm,
		}}
	default:
		request.Payload = &pb.ClientEnvelope_Login{Login: &pb.LoginRequest{
			Username: username,
			Password: password,
		}}
	}
	return request
}

func v1MessageFromEnvelope(request *pb.ClientEnvelope) (*pb.Message, error) {
	if request == nil {
		return nil, fmt.Errorf("V1 请求不能为空")
	}
	message := &pb.Message{}
	switch payload := request.GetPayload().(type) {
	case *pb.ClientEnvelope_Login:
		message.Type = protocol.TypeLogin
		message.Username = payload.Login.GetUsername()
		message.Password = payload.Login.GetPassword()
	case *pb.ClientEnvelope_Register:
		message.Type = protocol.TypeRegister
		message.Username = payload.Register.GetUsername()
		message.Password = payload.Register.GetPassword()
		message.Confirm = payload.Register.GetConfirmPassword()
	case *pb.ClientEnvelope_QuickEnter:
		message.Type = protocol.TypeQuickEnter
		message.Username = payload.QuickEnter.GetUsername()
		message.Password = payload.QuickEnter.GetPassword()
	case *pb.ClientEnvelope_Move:
		direction, ok := v1Direction(payload.Move.GetDirection())
		if !ok {
			return nil, fmt.Errorf("V1 不支持的移动方向")
		}
		message.Type = protocol.TypeMove
		message.Dir = direction
	case *pb.ClientEnvelope_Attack:
		message.Type = protocol.TypeAttack
	case *pb.ClientEnvelope_AttackBoss:
		message.Type = protocol.TypeBossAttack
	case *pb.ClientEnvelope_Heal:
		message.Type = protocol.TypeHeal
	case *pb.ClientEnvelope_BuyItem:
		message.Type = protocol.TypeShop
		message.Item = payload.BuyItem.GetItem()
	case *pb.ClientEnvelope_SwitchMap:
		message.Type = protocol.TypeSwitchMap
		message.MapId = payload.SwitchMap.GetMapId()
	case *pb.ClientEnvelope_Logout:
		message.Type = protocol.TypeLogout
	default:
		return nil, fmt.Errorf("V1 不支持该命令")
	}
	return message, nil
}

func v1EnvelopeFromMessage(message *pb.Message) *pb.ServerEnvelope {
	if message == nil {
		return &pb.ServerEnvelope{Payload: &pb.ServerEnvelope_Error{Error: &pb.ErrorResponse{
			Code:    pb.ErrorCode_ERROR_CODE_INTERNAL,
			Message: "V1 网关返回空消息",
		}}}
	}
	switch message.GetType() {
	case protocol.TypeAuth:
		return &pb.ServerEnvelope{RequestId: 1, Payload: &pb.ServerEnvelope_Authenticated{Authenticated: &pb.Authenticated{State: message.GetState()}}}
	case protocol.TypeState:
		return &pb.ServerEnvelope{Payload: &pb.ServerEnvelope_State{State: message.GetState()}}
	case protocol.TypeError:
		return &pb.ServerEnvelope{Payload: &pb.ServerEnvelope_Error{Error: &pb.ErrorResponse{
			Code:    pb.ErrorCode_ERROR_CODE_INTERNAL,
			Message: message.GetError(),
		}}}
	default:
		return &pb.ServerEnvelope{Payload: &pb.ServerEnvelope_Notice{Notice: &pb.ServerNotice{Message: message.GetText()}}}
	}
}

func v1Direction(direction pb.Direction) (string, bool) {
	switch direction {
	case pb.Direction_DIRECTION_UP:
		return protocol.DirUp, true
	case pb.Direction_DIRECTION_DOWN:
		return protocol.DirDown, true
	case pb.Direction_DIRECTION_LEFT:
		return protocol.DirLeft, true
	case pb.Direction_DIRECTION_RIGHT:
		return protocol.DirRight, true
	default:
		return "", false
	}
}

func isSupportedClientProtocol(protocolVersion string) bool {
	return protocolVersion == clientProtocolV1 || protocolVersion == clientProtocolV2
}

func formatGatewayError(requestID uint64, responseError *pb.ErrorResponse) string {
	if responseError == nil {
		return "网关返回未知错误"
	}
	prefix := "网关错误"
	if requestID != 0 {
		prefix = fmt.Sprintf("指令 #%d", requestID)
	}
	message := responseError.GetMessage()
	if message == "" {
		message = responseError.GetCode().String()
	}
	if responseError.GetRetryable() {
		return fmt.Sprintf("%s：%s（可重试）", prefix, message)
	}
	return fmt.Sprintf("%s：%s", prefix, message)
}

func shouldApplyState(current, next *pb.WorldState) bool {
	if next == nil {
		return false
	}
	if current == nil {
		return true
	}
	if next.GetSessionVersion() != current.GetSessionVersion() {
		return next.GetSessionVersion() > current.GetSessionVersion()
	}
	if next.GetTopologyVersion() != current.GetTopologyVersion() {
		return next.GetTopologyVersion() > current.GetTopologyVersion()
	}
	if next.GetMapEpoch() != current.GetMapEpoch() {
		return next.GetMapEpoch() > current.GetMapEpoch()
	}
	if next.GetMap().GetId() != current.GetMap().GetId() {
		return true
	}
	return next.GetMap().GetVersion() >= current.GetMap().GetVersion()
}

func applyState(state *pb.WorldState) bool {
	uiMu.Lock()
	defer uiMu.Unlock()
	if !shouldApplyState(current, state) {
		return false
	}
	current = state
	return true
}

func setState(state *pb.WorldState) {
	uiMu.Lock()
	defer uiMu.Unlock()
	current = state
}

func isShopOpen() bool {
	uiMu.Lock()
	defer uiMu.Unlock()
	return shopOpen
}

func setShopOpen(open bool) {
	uiMu.Lock()
	defer uiMu.Unlock()
	shopOpen = open
}

func addClientNote(text string) {
	if shouldSuppressClientNote(text) {
		return
	}
	uiMu.Lock()
	defer uiMu.Unlock()
	clientNotes = append(clientNotes, text)
	if len(clientNotes) > 5 {
		clientNotes = clientNotes[len(clientNotes)-5:]
	}
}

func shouldSuppressClientNote(text string) bool {
	switch text {
	case "向上移动", "向下移动", "向左移动", "向右移动",
		"发起近战攻击", "使用药剂", "挑战世界首领",
		"切换到青岚要塞", "切换到玄矿地窟", "切换到残星遗迹",
		"购买药剂", "强化武器":
		return true
	default:
		return false
	}
}

func drawUI() {
	uiMu.Lock()
	defer uiMu.Unlock()

	if current == nil {
		return
	}

	mapLines := buildMapLines(current)
	sideLines := buildSideLines(current)
	eventLines := buildEventLines(current)

	height := max(len(mapLines), len(sideLines))
	var sb strings.Builder
	sb.WriteString("\033[H")
	sb.WriteString(renderBanner("烬原战境 Lab3", "即时战斗 / 中文彩色界面 / 多地图并行 / 多节点协同"))
	sb.WriteString("\n")
	for i := 0; i < height; i++ {
		left := ""
		if i < len(mapLines) {
			left = mapLines[i]
		}
		right := ""
		if i < len(sideLines) {
			right = sideLines[i]
		}
		sb.WriteString(left)
		sb.WriteString("  ")
		sb.WriteString(right)
		sb.WriteString("\033[K\n")
	}
	sb.WriteString("\n")
	for _, line := range eventLines {
		sb.WriteString(line)
		sb.WriteString("\033[K\n")
	}
	if shopOpen {
		sb.WriteString(colorGold + "商店模式：" + colorReset + colorDim + "1 购买药剂  2 强化武器  P/Esc 关闭商店" + colorReset + "\n")
	} else {
		sb.WriteString(colorDim + "按键：W/A/S/D 移动  J 攻击  K 治疗  B 世界首领  P 商店  1/2/3 切图  R 刷新  Q 退出" + colorReset + "\n")
	}
	sb.WriteString("\033[J")
	fmt.Print(sb.String())
}

func buildMapLines(state *pb.WorldState) []string {
	mapView := state.GetMap()
	grid := make([][]rune, len(mapView.GetTerrain()))
	for y, row := range mapView.GetTerrain() {
		grid[y] = []rune(row)
	}
	for _, treasure := range mapView.GetTreasures() {
		if inBounds(grid, int(treasure.GetX()), int(treasure.GetY())) {
			grid[treasure.GetY()][treasure.GetX()] = '$'
		}
	}
	for _, npc := range mapView.GetNpcs() {
		if inBounds(grid, int(npc.GetX()), int(npc.GetY())) {
			grid[npc.GetY()][npc.GetX()] = 'n'
		}
	}
	if site, ok := bossSiteOnMap(state); ok && inBounds(grid, int(site.GetX()), int(site.GetY())) {
		if state.GetBoss().GetAlive() {
			grid[site.GetY()][site.GetX()] = 'b'
		} else {
			grid[site.GetY()][site.GetX()] = 'o'
		}
	}
	for _, player := range mapView.GetPlayers() {
		if !inBounds(grid, int(player.GetX()), int(player.GetY())) {
			continue
		}
		switch {
		case player.GetUsername() == state.GetSelf().GetUsername():
			grid[player.GetY()][player.GetX()] = '@'
		case !player.GetAlive():
			grid[player.GetY()][player.GetX()] = 'x'
		default:
			grid[player.GetY()][player.GetX()] = 'p'
		}
	}

	lines := []string{
		colorGold + "┏━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┓" + colorReset,
		colorGold + fmt.Sprintf("┃ 地图：%-12s 节点：%-8s 版本：%-6d ┃", mapView.GetName(), mapView.GetNodeId(), mapView.GetVersion()) + colorReset,
		colorGold + "┣━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┫" + colorReset,
	}
	for _, row := range grid {
		var line strings.Builder
		line.WriteString(colorGold + "┃" + colorReset)
		for _, cell := range row {
			line.WriteString(paintCell(cell))
		}
		line.WriteString(colorGold + "┃" + colorReset)
		lines = append(lines, line.String())
	}
	lines = append(lines, colorGold+"┗━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┛"+colorReset)
	return lines
}

func buildSideLines(state *pb.WorldState) []string {
	self := state.GetSelf()
	boss := state.GetBoss()
	lines := []string{
		panelTitle("勇士面板"),
		panelLine("角色", colorCyan+colorBold+self.GetUsername()+colorReset),
		panelLine("位置", fmt.Sprintf("(%d,%d)", self.GetX(), self.GetY())),
		panelLine("生命", hpBar(int(self.GetHp()), int(self.GetMaxHp()), 18, colorGreen)),
		panelLine("攻击", fmt.Sprintf("%d", self.GetAttack())),
		panelLine("药剂", fmt.Sprintf("%d", self.GetPotions())),
		panelLine("战利品", fmt.Sprintf("%d", self.GetTreasures())),
		panelLine("击杀", fmt.Sprintf("%d", self.GetKills())),
		panelLine("阵亡", fmt.Sprintf("%d", self.GetDeaths())),
		panelLine("胜场", fmt.Sprintf("%d", self.GetVictories())),
	}
	if !self.GetAlive() {
		lines = append(lines, panelLine("复活", colorOrange+fmt.Sprintf("%d 秒", self.GetRespawnIn())+colorReset))
	}
	lines = append(lines, panelTitle("战备商店"))
	if shopOpen {
		lines = append(lines,
			panelLine("1号商品", fmt.Sprintf("药剂 +1  价格 %d", protocol.PotionPrice)),
			panelLine("2号商品", fmt.Sprintf("武器强化 +%d  价格 %d", protocol.WeaponBoost, protocol.WeaponPrice)),
			panelLine("提示", colorGold+"按 1/2 立即购买"+colorReset),
		)
	} else {
		lines = append(lines, panelLine("入口", colorGold+"按 P 打开战备商店"+colorReset))
	}
	lines = append(lines, panelTitle("世界首领"))

	bossStatus := colorRed + "征战中" + colorReset
	if !boss.GetAlive() {
		bossStatus = colorGray + fmt.Sprintf("重组中 %ds", boss.GetRespawnIn()) + colorReset
	}
	siteText := "当前地图无投影"
	distText := "-"
	if site, ok := bossSiteOnMap(state); ok {
		siteText = fmt.Sprintf("(%d,%d)", site.GetX(), site.GetY())
		distText = fmt.Sprintf("%d", manhattan(int(self.GetX()), int(self.GetY()), int(site.GetX()), int(site.GetY())))
	}
	lastHit := boss.GetLastHit()
	if lastHit == "" {
		lastHit = "暂无"
	}
	lines = append(lines,
		panelLine("首领", colorOrange+colorBold+boss.GetName()+colorReset),
		panelLine("状态", bossStatus),
		panelLine("生命", hpBar(int(boss.GetHp()), int(boss.GetMaxHp()), 18, colorRed)),
		panelLine("投影", siteText),
		panelLine("距离", distText),
		panelLine("开战线", fmt.Sprintf("%d 格", boss.GetAttackGap())),
		panelLine("终结", lastHit),
		panelTitle("地图并行"),
	)

	for _, brief := range state.GetMaps() {
		marker := colorDim + "○" + colorReset
		if brief.GetIsCurrent() {
			marker = colorGold + "◆" + colorReset
		}
		lines = append(lines, fmt.Sprintf("%s %-5s %-8s 玩家:%d 怪:%d 宝:%d",
			marker, brief.GetName(), brief.GetNodeId(), brief.GetPlayers(), brief.GetNpcs(), brief.GetTreasures()))
	}

	lines = append(lines, panelTitle("节点心跳"))
	for _, node := range state.GetNodes() {
		status := colorRed + "离线" + colorReset
		if node.GetHealthy() {
			status = colorGreen + "在线" + colorReset
		}
		lines = append(lines, fmt.Sprintf("• %-8s %s 主:%d 备:%d",
			node.GetId(), status, len(node.GetPrimaryMaps()), len(node.GetReplicaMaps())))
	}
	return lines
}

func buildEventLines(state *pb.WorldState) []string {
	lines := []string{panelTitle("战场播报")}
	events := append([]string(nil), state.GetEvents()...)
	events = append(events, clientNotes...)
	start := 0
	if len(events) > 8 {
		start = len(events) - 8
	}
	for _, event := range events[start:] {
		lines = append(lines, colorGray+"│ "+colorReset+event)
	}
	return lines
}

func renderBanner(title, subtitle string) string {
	return colorBold + colorGold + "╔══════════════════════════════════════════════════════════════════════════════╗\n" +
		"║ " + title + strings.Repeat(" ", max(0, 74-len([]rune(title)))) + "║\n" +
		"║ " + colorReset + colorDim + subtitle + colorGold + strings.Repeat(" ", max(0, 74-len([]rune(subtitle)))) + "║\n" +
		"╚══════════════════════════════════════════════════════════════════════════════╝" + colorReset
}

func panelTitle(title string) string {
	return bgPanel + colorBold + " " + title + " " + colorReset
}

func panelLine(label, value string) string {
	return fmt.Sprintf("%s%-6s%s %s", colorDim, label+"：", colorReset, value)
}

func hpBar(hp, maxHP, width int, color string) string {
	if maxHP <= 0 {
		maxHP = 1
	}
	if hp < 0 {
		hp = 0
	}
	filled := hp * width / maxHP
	return fmt.Sprintf("[%s%s%s%s]%s %4d/%-4d%s",
		color, strings.Repeat("█", filled), colorGray, strings.Repeat("░", width-filled), colorReset, hp, maxHP, colorReset)
}

func paintCell(cell rune) string {
	switch cell {
	case '#':
		return colorGray + "▓" + colorReset
	case '$':
		return colorGold + "✦" + colorReset
	case 'n':
		return colorOrange + "♞" + colorReset
	case 'b':
		return colorRed + colorBold + "♛" + colorReset
	case 'o':
		return colorDim + "◉" + colorReset
	case '@':
		return colorCyan + colorBold + "◆" + colorReset
	case 'p':
		return colorBlue + "◎" + colorReset
	case 'x':
		return colorDim + "☓" + colorReset
	default:
		return colorDim + "·" + colorReset
	}
}

func bossSiteOnMap(state *pb.WorldState) (*pb.BossSite, bool) {
	for _, site := range state.GetBoss().GetSites() {
		if site.GetMapId() == state.GetMap().GetId() {
			return site, true
		}
	}
	return nil, false
}

func inBounds(grid [][]rune, x, y int) bool {
	return y >= 0 && y < len(grid) && x >= 0 && x < len(grid[y])
}

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
		mode := protocol.TypeLogin
		switch choice {
		case "1", "":
			mode = protocol.TypeLogin
		case "2":
			mode = protocol.TypeRegister
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
		if mode == protocol.TypeRegister {
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

func manhattan(ax, ay, bx, by int) int {
	dx := ax - bx
	if dx < 0 {
		dx = -dx
	}
	dy := ay - by
	if dy < 0 {
		dy = -dy
	}
	return dx + dy
}
