package main

import (
	"context"
	"crypto/tls"
	"encoding/hex"
	"fmt"
	"math/rand"
	"net/url"
	"os"
	"time"

	"github.com/alecthomas/kong"
	"github.com/apache/arrow/go/v12/arrow/flight"
	"github.com/apache/arrow/go/v12/arrow/flight/flightsql"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
)

const (
	pgTimestampFormat = "2006-01-02 15:04:05.999999999"
	traceIDHeader     = "influx-trace-id"
	traceIDHeader2    = "uber-trace-id"
)

// Context is a CLI context.
type Context struct {
	*CLI
}

// CLI contains the CLI parameters.
type CLI struct {
	URL   string `required:""`
	DB    string `required:""`
	Token string `env:"FLIGHT_CLUB_TOKEN"`

	Headers    map[string]string `short:"H" env:"FLIGHT_CLUB_HEADERS"`
	GenTraceId bool

	Query QueryCmd `cmd:"" help:"query"`
}

type QueryCmd struct {
	Query      string   `arg:"" help:"Query text"`
	SkipWarmup bool     `optional:"" help:"Skip warmup request"`
	Output     *os.File `short:"o" optional:"" help:"filename where output is printed"`
}

func (cmd *QueryCmd) Run(cli *Context) error {
	ctx := metadata.AppendToOutgoingContext(context.Background(),
		"database", cli.DB,
		// we need to pass this explicitly because IOx doesn't support the `auth-token` header that flight passes
		"authorization", "Token "+cli.Token,
		// enables special queries
		"iox-debug", "true",
	)
	ctx = metadata.AppendToOutgoingContext(ctx, cli.customHeaders()...)

	if cli.GenTraceId {
		traceID := generateRandomHex(8)
		traceHeader := fmt.Sprintf("%s:1112223334445:0:1", traceID)
		ctx = metadata.AppendToOutgoingContext(ctx,
			traceIDHeader, traceHeader,
			traceIDHeader2, traceHeader,
		)

		fmt.Printf("Trace ID set to %s\n", traceID)
	}

	addr, cred, err := parseAddr(cli.URL)
	if err != nil {
		return err
	}
	c, err := flightsql.NewClientCtx(ctx, addr, cli, nil, grpc.WithTransportCredentials(cred))
	if err != nil {
		return err
	}

	// Some time is spend on the first flight request, whatever that request is, let's run a dummy request first
	// so that we can better measure the other ones
	beforeWarmup := time.Now()
	if !cmd.SkipWarmup {
		if _, err := c.GetCatalogs(ctx); err != nil {
			return err
		}
	}
	warmupDuration := time.Since(beforeWarmup)

	w := os.Stdout
	if cmd.Output != nil {
		w = cmd.Output
	}
	timings, err := printQuery(ctx, w, c, cmd.Query)
	if err != nil {
		return err
	}

	fmt.Println()
	fmt.Print(timings.Add(Timings{Warmup: warmupDuration}))

	return nil
}

func (cli *CLI) customHeaders() (pairs []string) {
	for k, v := range cli.Headers {
		pairs = append(pairs, k)
		pairs = append(pairs, v)
	}
	return pairs
}

func (cli *CLI) Authenticate(context.Context, flight.AuthConn) error {
	return fmt.Errorf("not implemented")
}
func (cli *CLI) GetToken(context.Context) (string, error) {
	return cli.Token, nil
}

func parseAddr(s string) (string, credentials.TransportCredentials, error) {
	u, err := url.Parse(s)
	if err != nil {
		return "", nil, err
	}
	p := u.Port()
	if p == "" {
		if u.Scheme == "https" {
			p = "443"
		} else if u.Scheme == "http" {
			p = "80"
		}
	}
	a := fmt.Sprintf("%s:%s", u.Hostname(), p)
	switch u.Scheme {
	case "http":
		return a, insecure.NewCredentials(), nil
	case "https":
		return a, credentials.NewTLS(&tls.Config{}), nil
	default:
		return "", nil, fmt.Errorf("unhandled schema %q", u.Scheme)
	}
}

func generateRandomHex(n int) string {
	bytes := make([]byte, n)
	rand.Seed(time.Now().UnixNano())
	rand.Read(bytes)

	return hex.EncodeToString(bytes)
}

func main() {
	var cli CLI
	ctx := kong.Parse(&cli)
	err := ctx.Run(&Context{CLI: &cli})
	ctx.FatalIfErrorf(err)
}
