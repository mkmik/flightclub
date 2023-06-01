package main

import (
	"context"
	"fmt"
	"io"
	"os"
	"time"

	"github.com/apache/arrow/go/v12/arrow"
	"github.com/apache/arrow/go/v12/arrow/array"
	"github.com/apache/arrow/go/v12/arrow/flight"
	"github.com/apache/arrow/go/v12/arrow/flight/flightsql"
	"github.com/olekukonko/tablewriter"
)

type Timings struct {
	Warmup  time.Duration
	Execute time.Duration
	DoGet   time.Duration
}

func (t *Timings) Add(other Timings) Timings {
	t.Warmup += other.Warmup
	t.Execute += other.Execute
	t.DoGet += other.DoGet

	return *t
}

func (t Timings) String() string {
	return fmt.Sprintf("Warmup: %s, Execute: %s, DoGet: %s, Total: %s\n",
		t.Warmup, t.Execute, t.DoGet,
		t.Total())
}

func (t *Timings) Total() time.Duration {
	return t.Warmup + t.Execute + t.DoGet
}

func printQuery(ctx context.Context, c *flightsql.Client, query string) (Timings, error) {
	beforeExecute := time.Now()
	info, err := c.Execute(ctx, query)
	if err != nil {
		return Timings{}, err
	}
	executeDuration := time.Since(beforeExecute)

	timings, err := printInfo(ctx, c, info)
	if err != nil {
		return Timings{}, err
	}

	return timings.Add(Timings{Execute: executeDuration}), nil
}

func printInfo(ctx context.Context, c *flightsql.Client, info *flight.FlightInfo) (Timings, error) {
	var doGetDuration time.Duration

	for _, endpoint := range info.Endpoint {
		beforeDoGet := time.Now()
		reader, err := c.DoGet(ctx, endpoint.GetTicket())
		if err != nil {
			return Timings{}, fmt.Errorf("getting ticket failed: %w", err)
		}
		doGetDuration += time.Since(beforeDoGet)

		for reader.Next() {
			record := reader.Record()
			if err := printRecord(record); err != nil {
				return Timings{}, err
			}
		}
		reader.Release()

		if err := reader.Err(); err != nil {
			if err == io.EOF {
				break
			}
			return Timings{}, err
		}
	}
	timings := Timings{
		DoGet: doGetDuration,
	}
	return timings, nil
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
