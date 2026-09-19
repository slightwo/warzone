package world

import "battleworld/protocol"

func AvailableMaps() []MapConfig {
	return []MapConfig{
		buildGreenMap(),
		buildCaveMap(),
		buildRuinsMap(),
	}
}

func DefaultMapID() string {
	return "green"
}

func FindConfig(id string) (MapConfig, bool) {
	for _, cfg := range AvailableMaps() {
		if cfg.ID == id {
			return cfg, true
		}
	}
	return MapConfig{}, false
}

func stringsToGrid(rows []string) [][]rune {
	grid := make([][]rune, len(rows))
	for i, row := range rows {
		grid[i] = []rune(row)
	}
	return grid
}

func gridToStrings(grid [][]rune) []string {
	rows := make([]string, len(grid))
	for i, row := range grid {
		rows[i] = string(row)
	}
	return rows
}

func blankGrid() [][]rune {
	grid := make([][]rune, protocol.MapHeight)
	for y := range grid {
		grid[y] = make([]rune, protocol.MapWidth)
		for x := range grid[y] {
			grid[y][x] = '.'
		}
	}
	return grid
}

func drawBorder(grid [][]rune) {
	for x := 0; x < len(grid[0]); x++ {
		grid[0][x] = '#'
		grid[len(grid)-1][x] = '#'
	}
	for y := 0; y < len(grid); y++ {
		grid[y][0] = '#'
		grid[y][len(grid[y])-1] = '#'
	}
}

func fillRect(grid [][]rune, x, y, width, height int) {
	for yy := y; yy < y+height && yy < len(grid); yy++ {
		for xx := x; xx < x+width && xx < len(grid[yy]); xx++ {
			grid[yy][xx] = '#'
		}
	}
}

func drawH(grid [][]rune, y, fromX, toX int) {
	if y < 0 || y >= len(grid) {
		return
	}
	for x := max(0, fromX); x <= toX && x < len(grid[y]); x++ {
		grid[y][x] = '#'
	}
}

func drawV(grid [][]rune, x, fromY, toY int) {
	for y := max(0, fromY); y <= toY && y < len(grid); y++ {
		if x >= 0 && x < len(grid[y]) {
			grid[y][x] = '#'
		}
	}
}

func carve(grid [][]rune, x, y int) {
	if y >= 0 && y < len(grid) && x >= 0 && x < len(grid[y]) {
		grid[y][x] = '.'
	}
}

func buildGreenMap() MapConfig {
	grid := blankGrid()
	drawBorder(grid)
	fillRect(grid, 8, 3, 14, 5)
	fillRect(grid, 33, 14, 14, 4)
	drawV(grid, 26, 1, 21)
	drawV(grid, 45, 2, 13)
	drawH(grid, 9, 1, 20)
	drawH(grid, 7, 30, 54)
	carve(grid, 26, 5)
	carve(grid, 26, 12)
	carve(grid, 26, 18)
	carve(grid, 45, 5)
	carve(grid, 45, 10)
	carve(grid, 6, 9)
	carve(grid, 12, 9)
	carve(grid, 18, 9)
	carve(grid, 37, 7)
	carve(grid, 44, 7)
	carve(grid, 50, 7)
	return MapConfig{
		ID:     "green",
		Name:   "青岚要塞",
		Layout: gridToStrings(grid),
		SpawnX: 4,
		SpawnY: 4,
		BossX:  50,
		BossY:  20,
	}
}

func buildCaveMap() MapConfig {
	grid := blankGrid()
	drawBorder(grid)
	fillRect(grid, 5, 5, 10, 9)
	fillRect(grid, 36, 12, 12, 7)
	drawV(grid, 28, 1, 22)
	drawH(grid, 4, 18, 51)
	drawH(grid, 18, 2, 25)
	drawV(grid, 18, 10, 20)
	carve(grid, 28, 4)
	carve(grid, 28, 11)
	carve(grid, 28, 18)
	carve(grid, 22, 4)
	carve(grid, 34, 4)
	carve(grid, 45, 4)
	carve(grid, 9, 18)
	carve(grid, 14, 18)
	carve(grid, 18, 15)
	return MapConfig{
		ID:     "cave",
		Name:   "玄矿地窟",
		Layout: gridToStrings(grid),
		SpawnX: 4,
		SpawnY: 20,
		BossX:  49,
		BossY:  20,
	}
}

func buildRuinsMap() MapConfig {
	grid := blankGrid()
	drawBorder(grid)
	drawV(grid, 12, 2, 20)
	drawV(grid, 24, 1, 18)
	drawV(grid, 37, 5, 22)
	drawH(grid, 6, 2, 22)
	drawH(grid, 13, 15, 40)
	fillRect(grid, 42, 3, 8, 5)
	fillRect(grid, 6, 15, 8, 4)
	carve(grid, 12, 5)
	carve(grid, 12, 11)
	carve(grid, 12, 17)
	carve(grid, 24, 4)
	carve(grid, 24, 10)
	carve(grid, 24, 16)
	carve(grid, 37, 9)
	carve(grid, 37, 17)
	carve(grid, 8, 6)
	carve(grid, 18, 6)
	carve(grid, 19, 13)
	carve(grid, 29, 13)
	carve(grid, 44, 13)
	return MapConfig{
		ID:     "ruins",
		Name:   "残星遗迹",
		Layout: gridToStrings(grid),
		SpawnX: 50,
		SpawnY: 4,
		BossX:  50,
		BossY:  20,
	}
}
