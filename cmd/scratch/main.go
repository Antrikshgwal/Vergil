package main

import (
	"context"
	"fmt"
	"time"

	"github.com/Antrikshgwal/Vergil.git/internal/feature"
)

func main() {
	store := feature.NewRedisStore("localhost:6379", 60*time.Second)
	ctx := context.Background()
	for i := 0; i < 3; i++ {
		n, err := store.Velocity(ctx, "u1")
		if err != nil {
			panic(err)
		}
		fmt.Println("count:", n)
	}
}
