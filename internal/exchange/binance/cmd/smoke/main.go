// Command smoke connects to Binance USDT-M futures for 30 seconds and prints
// every event for the given symbols (default BTCUSDT):
//
//	go run ./internal/exchange/binance/cmd/smoke BTCUSDT ETHUSDT
package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"time"

	"trader-bot/internal/clock"
	"trader-bot/internal/exchange/binance"
	"trader-bot/internal/market"
)

func main() {
	want := os.Args[1:]
	if len(want) == 0 {
		want = []string{"BTCUSDT"}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	rest := binance.NewREST(binance.DefaultRESTURL, binance.DefaultWeightPerMin)
	have, err := rest.Symbols(ctx)
	if err != nil {
		slog.Error("exchangeInfo", "err", err)
		os.Exit(1)
	}
	syms, dropped := binance.ValidateSymbols(want, have)
	fmt.Printf("perpetual USDT symbols: %d; watching %v; dropped %v\n", len(have), syms, dropped)
	if len(syms) == 0 {
		os.Exit(1)
	}
	if ks, err := rest.Klines(ctx, syms[0], market.I1m, time.Now().Add(-3*time.Minute), 3); err == nil {
		for _, k := range ks {
			fmt.Printf("rest   %+v\n", k)
		}
	}

	out := make(chan market.Event, 256)
	symbols := func() []string { return syms }
	clk := clock.Real{}
	st := binance.NewStream(binance.DefaultWSURL, rest, clk, func(up bool) { fmt.Println("state  connected =", up) })
	go st.Run(ctx, symbols, nil, out)
	go binance.PollContract(ctx, rest, symbols, 5*time.Minute, out, clk)
	for {
		select {
		case <-ctx.Done():
			return
		case ev := <-out:
			fmt.Printf("%-6s %T %+v\n", "event", ev, ev)
		}
	}
}
