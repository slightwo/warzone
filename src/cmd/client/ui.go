package main

import (
	"fmt"
	"strings"
	"sync"

	"battleworld/pb"
	"battleworld/protocol"
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

var (
	uiMu        sync.Mutex
	current     *pb.WorldState
	clientNotes []string
	shopOpen    bool
)

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
		lines = append(lines, fmt.Sprintf("• %-8s %s Owner:%d",
			node.GetId(), status, len(node.GetPrimaryMaps())))
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
