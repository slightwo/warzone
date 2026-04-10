package main
import (
"database/sql"
"fmt"
_ "github.com/lib/pq"
)
func main() {
_, err := sql.Open("postgres", "host=localhost user=postgres password=Wu050601&& dbname=battleworld port=5432 sslmode=disable")
fmt.Println(err)
}
