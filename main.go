package main

import (
	"context"
	"crypto/tls"
	"fmt"
	"io"
	"log"
	"net/url"
	"os"
	"time"

	"github.com/alecthomas/kong"
	"github.com/apache/arrow/go/v12/arrow"
	"github.com/apache/arrow/go/v12/arrow/array"
	"github.com/apache/arrow/go/v12/arrow/flight"
	"github.com/apache/arrow/go/v12/arrow/flight/flightsql"
	"github.com/olekukonko/tablewriter"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
)

const (
	pgTimestampFormat = "2006-01-02 15:04:05.999999999"
)

// Context is a CLI context.
type Context struct {
	*CLI
}

// CLI contains the CLI parameters.
type CLI struct {
	URL   string `required:""`
	DB    string `required:""`
	Token string `env:"FLIGHT_TOOL_TOKEN"`

	Query QueryCmd `cmd:"" help:"query"`
}

type QueryCmd struct {
	Query string `arg:"" help:"Query text"`
}

func (cmd *QueryCmd) Run(cli *Context) error {
	ctx := metadata.AppendToOutgoingContext(context.Background(),
		"database", cli.DB,
		// we need to pass this explicitly because IOx doesn't support the `auth-token` header that flight passes
		"authorization", "Token "+cli.Token,
		// enables special queries
		"iox-debug", "true",
	)

	addr, cred, err := parseAddr(cli.URL)
	if err != nil {
		return err
	}
	c, err := flightsql.NewClientCtx(ctx, addr, cli, nil, grpc.WithTransportCredentials(cred))
	if err != nil {
		return err
	}

	log.Printf("executing")
	info, err := c.Execute(ctx, cmd.Query)
	if err != nil {
		return err
	}
	log.Printf("getting reader")
	for _, endpoint := range info.Endpoint {
		reader, err := c.DoGet(ctx, endpoint.GetTicket())
		if err != nil {
			return fmt.Errorf("getting ticket failed: %w", err)
		}
		for reader.Next() {
			record := reader.Record()
			if err := printRecord(record); err != nil {
				return err
			}
		}
		reader.Release()

		if err := reader.Err(); err != nil {
			if err == io.EOF {
				break
			}
			return err
		}
	}

	return nil
}

func printRecord(record arrow.Record) error {
	table := tablewriter.NewWriter(os.Stdout)
	table.SetAutoFormatHeaders(false)
	table.SetRowLine(false)
	table.SetBorder(false)
	table.SetAutoWrapText(true)
	//	table.SetBorders(tablewriter.Border{Top: true})

	var header []string
	for c := 0; c < int(record.NumCols()); c++ {
		header = append(header, record.ColumnName(c))
	}
	table.SetHeader(header)

	for r := 0; r < int(record.NumRows()); r++ {
		var row []string
		for c := 0; c < int(record.NumCols()); c++ {
			s, err := renderText(record.Column(c), r)
			if err != nil {
				return err
			}

			row = append(row, s)
		}
		table.Append(row)
	}

	table.SetFooter(header)
	table.Render()
	return nil
}

func renderText(column arrow.Array, row int) (string, error) {
	if column.IsNull(row) {
		return "NULL", nil
	}
	switch typedColumn := column.(type) {
	case *array.Timestamp:
		unit := typedColumn.DataType().(*arrow.TimestampType).Unit
		return typedColumn.Value(row).ToTime(unit).Format(pgTimestampFormat), nil
	case *array.Time32:
		unit := typedColumn.DataType().(*arrow.Time32Type).Unit
		return typedColumn.Value(row).ToTime(unit).Format(pgTimestampFormat), nil
	case *array.Time64:
		unit := typedColumn.DataType().(*arrow.Time64Type).Unit
		return typedColumn.Value(row).ToTime(unit).Format(pgTimestampFormat), nil
	case *array.Date32:
		return typedColumn.Value(row).ToTime().Format(pgTimestampFormat), nil
	case *array.Date64:
		return typedColumn.Value(row).ToTime().Format(pgTimestampFormat), nil
	case *array.Duration:
		m := typedColumn.DataType().(*arrow.DurationType).Unit.Multiplier()
		return (time.Duration(typedColumn.Value(row)) * m).String(), nil
	case *array.Float16:
		return fmt.Sprint(typedColumn.Value(row)), nil
	case *array.Float32:
		return fmt.Sprint(typedColumn.Value(row)), nil
	case *array.Float64:
		return fmt.Sprint(typedColumn.Value(row)), nil
	case *array.Uint8:
		return fmt.Sprint(typedColumn.Value(row)), nil
	case *array.Uint16:
		return fmt.Sprint(typedColumn.Value(row)), nil
	case *array.Uint32:
		return fmt.Sprint(typedColumn.Value(row)), nil
	case *array.Uint64:
		return fmt.Sprint(typedColumn.Value(row)), nil
	case *array.Int8:
		return fmt.Sprint(typedColumn.Value(row)), nil
	case *array.Int16:
		return fmt.Sprint(typedColumn.Value(row)), nil
	case *array.Int32:
		return fmt.Sprint(typedColumn.Value(row)), nil
	case *array.Int64:
		return fmt.Sprint(typedColumn.Value(row)), nil
	case *array.String:
		return typedColumn.Value(row), nil
	case *array.Binary:
		return fmt.Sprint(typedColumn.Value(row)), nil
	case *array.Boolean:
		if typedColumn.Value(row) {
			return "t", nil
		} else {
			return "f", nil
		}
	default:
		return "", fmt.Errorf("unsupported arrow type %q", column.DataType().Name())
	}
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

func main() {
	var cli CLI
	ctx := kong.Parse(&cli)
	err := ctx.Run(&Context{CLI: &cli})
	ctx.FatalIfErrorf(err)
}
