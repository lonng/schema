package main

import (
	"bytes"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"math"
	"math/rand"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/parser"
	"github.com/pingcap/parser/ast"
	"github.com/pingcap/parser/format"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/parser/types"
	_ "github.com/pingcap/tidb/types/parser_driver"
)

const (
	sizeTinyBlob   = 100
	sizeMediumBlob = 200
	sizeLongBlob   = 200
)

func init() {
	rand.Seed(time.Now().UnixNano())
}

var cliArgs struct {
	ImportFilePath string
	OutputFilePath string
	TableSizeFile  string
	Test           bool
}

var sizeMapper = []struct {
	UnitName string
	Unit     int64
}{
	{UnitName: "TB", Unit: 1 << 40},
	{UnitName: "GB", Unit: 1 << 30},
	{UnitName: "MB", Unit: 1 << 20},
	{UnitName: "KB", Unit: 1 << 10},
	{UnitName: "B", Unit: 1},
}

func main() {
	flag.StringVar(&cliArgs.ImportFilePath, "i", "", "mysql schema file")
	flag.StringVar(&cliArgs.OutputFilePath, "o", "", "template output file")
	flag.StringVar(&cliArgs.TableSizeFile, "size", "", "table size")
	flag.BoolVar(&cliArgs.Test, "test", false, "generate test template")
	flag.Parse()
	if cliArgs.ImportFilePath == "" {
		log.Fatal("mysql schema file should not empty")
	}

	tableSizeStats := map[string]int64{}
	size, err := ioutil.ReadFile(cliArgs.TableSizeFile)
	if err != nil {
		log.Fatalf("open table size file failed: %v", err)
	}
	lines := strings.Split(string(size), "\n")
	for _, line := range lines {
		parts := strings.Split(line, "\t")
		if len(parts) < 2 {
			log.Println("warning: invalid size spec: " + line)
			continue
		}
		tableName := parts[0]
		tableSize := parts[1]
		if strings.Contains(tableName, ".") {
			nameParts := strings.Split(tableName, ".")
			tableName = nameParts[len(nameParts)-1]
		}
		for _, m := range sizeMapper {
			suffix := m.UnitName
			if !strings.HasSuffix(tableSize, suffix) {
				continue
			}
			tableSize = strings.TrimSuffix(tableSize, suffix)
			parsedSize, err := strconv.ParseFloat(tableSize, 64)
			if err != nil {
				log.Println("warning: invalid table size: "+tableSize, suffix)
				continue
			}
			size := int64(parsedSize * float64(m.Unit))
			log.Printf("table `%v` size: %v%s, in bytes: %vbytes (parsed: %v, unit: %v)\n",
				tableName, tableSize, suffix, size, parsedSize, m.Unit)
			tableSizeStats[tableName] = size
		}
	}

	sql, err := ioutil.ReadFile(cliArgs.ImportFilePath)
	if err != nil {
		log.Fatalf("read schema file failed: %v", err)
	}

	out, err := os.OpenFile(cliArgs.OutputFilePath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, os.ModePerm)
	if err != nil {
		log.Fatalf("open output file failed: %v", err)
	}
	defer out.Close()

	p := parser.New()
	stmts, warns, err := p.Parse(string(sql), "", "")
	if err != nil {
		log.Fatalf("parse schema file failed: %v", err)
	}
	for _, w := range warns {
		log.Println("warn: " + w.Error())
	}

	tableCountStats := map[string]int{}

	// shell header
	fmt.Fprintln(out, "#!/bin/sh")
	fmt.Fprintf(out, "echo 'GENERATED AT %s'\n", time.Now().String())
	fmt.Fprintln(out, "echo 'CREATE SCHEMA 'db1903_baofu';' > db1903_baofu-schema-create.sql")

	var restoreTableCount int

CreateTable:
	for _, stmt := range stmts {
		createTable, ok := stmt.(*ast.CreateTableStmt)
		if !ok {
			log.Printf("statement %T is not create table", stmt)
			continue
		}

		tableCountStats[createTable.Table.Name.L]++
		if tableCountStats[createTable.Table.Name.L] > 1 {
			log.Printf("duplicated table: %s, found: %d\n", createTable.Table.Name.L, tableCountStats[createTable.Table.Name.L])
			continue
		}

		var rowSize int
		var foundPK bool
		for _, col := range createTable.Cols {
			ft := col.Tp
			// Will skip tables which primary key is Tinyint or Short because of small table
			// maybe cause primary key deplicate in generated data
			isSkipColumn := ft.Tp == mysql.TypeTiny || ft.Tp == mysql.TypeShort ||
				(ft.Tp == mysql.TypeVarchar && ft.Flen < 20) || (ft.Tp == mysql.TypeString && ft.Flen < 20)
			for _, cons := range createTable.Constraints {
				if cons.Tp != ast.ConstraintPrimaryKey {
					continue
				}
				if len(cons.Keys) == 1 && cons.Keys[0].Column.Name.L == col.Name.Name.L && isSkipColumn {
					// Skip the table
					continue CreateTable
				}
			}

			// Skip the column
			if isSkipColumn {
				continue
			}
			for _, opt := range col.Options {
				switch opt.Tp {
				case ast.ColumnOptionNotNull:
					col.Tp.Flag |= mysql.NotNullFlag
				case ast.ColumnOptionAutoIncrement:
					col.Tp.Flag |= mysql.AutoIncrementFlag
				}
			}
			for _, cons := range createTable.Constraints {
				if cons.Tp == ast.ConstraintPrimaryKey ||
					cons.Tp == ast.ConstraintUniqIndex ||
					cons.Tp == ast.ConstraintUniqKey ||
					cons.Tp == ast.ConstraintUniq {
					for _, indexCol := range cons.Keys {
						if indexCol.Column.Name.L == col.Name.Name.L {
							col.Tp.Flag |= mysql.PriKeyFlag
							foundPK = true
						}
					}
				}
			}

			var displayFlen, displayDecimal int
			switch ft.Tp {
			case mysql.TypeTinyBlob:
				displayFlen = sizeTinyBlob
			case mysql.TypeMediumBlob:
				displayFlen = sizeMediumBlob
			case mysql.TypeLongBlob:
				displayFlen = sizeLongBlob
			default:
				defaultFlen, defaultDecimal := mysql.GetDefaultFieldLengthAndDecimal(ft.Tp)
				// displayFlen and displayDecimal are flen and decimal values with `-1` substituted with default value.
				displayFlen, displayDecimal = ft.Flen, ft.Decimal
				if displayFlen == 0 || displayFlen == types.UnspecifiedLength {
					ft.Flen = defaultFlen
					displayFlen = defaultFlen
				}
				if displayDecimal == 0 || displayDecimal == types.UnspecifiedLength {
					ft.Decimal = defaultDecimal
					displayDecimal = defaultDecimal
				}
			}
			if mysql.HasAutoIncrementFlag(col.Tp.Flag) || mysql.HasPriKeyFlag(col.Tp.Flag) {
				rowSize += 8 // rownum is bigint
			} else if ft.Tp == mysql.TypeDecimal || ft.Tp == mysql.TypeNewDecimal {
				rowSize += displayFlen + displayDecimal
			} else {
				rowSize += displayFlen
			}
		}

		if !foundPK {
			log.Printf("Skip table %s because of the table primary key not found\n", createTable.Table.Name.L)
			_ = createTable.Restore(format.NewRestoreCtx(format.DefaultRestoreFlags, os.Stderr))
			continue
		}

		const MB = 1 << 20
		const FileSize = 256 * MB
		const rowCount = 100
		const RndMin = 20 * MB
		const RndRng = 512 * MB
		tableSize, found := tableSizeStats[createTable.Table.Name.O]
		if !found {
			tableSize = RndMin + rand.Int63n(RndRng)
			log.Printf("table %v size not define, generate random size: %d\n", createTable.Table.Name.O, tableSize)
		}

		if rowSize == 0 {
			log.Println("Skip rowsize 0 table", createTable.Table.Name.L)
			continue
		}
		insertCount := int64(FileSize / (rowCount * rowSize))
		if insertCount == 0 {
			insertCount = 1
		}

		// for test
		if cliArgs.Test {
			tableSize = 3 * MB
			insertCount = 1
		}

		filesCount := tableSize / insertCount / int64(rowCount*rowSize)
		if filesCount < 1 {
			filesCount = 1
		}
		fmt.Fprintf(out, "echo 'TABLE: %s, rowSize: %d insertCount: %v, tableSize: %v, filesCount:%v'\n",
			createTable.Table.Name.O, rowSize, insertCount, tableSize, filesCount)
		fmt.Fprintf(out, "dbgen -i /dev/stdin -o . -t 'db1903_baofu.`%s`' -n %d -r %d -k %d -j 40 --escape-backslash --time-zone Asia/Shanghai <<'SCHEMAEOF'\n",
			createTable.Table.Name.O, insertCount, rowCount, filesCount)
		buf := &bytes.Buffer{}
		ctx := format.NewRestoreCtx(format.DefaultRestoreFlags, buf)
		err := restore(ctx, createTable)
		if err != nil {
			log.Printf("restore error: %v", err)
		}
		out.Write(buf.Bytes())
		fmt.Fprintln(out, "SCHEMAEOF")

		restoreTableCount++
	}
	log.Println("Restore table count", restoreTableCount)
}

func restore(ctx *format.RestoreCtx, n *ast.CreateTableStmt) error {
	ctx.WriteKeyWord("CREATE TABLE _")
	ctx.WritePlain(" ")
	lenCols := len(n.Cols)
	lenConstraints := len(n.Constraints)
	if lenCols+lenConstraints > 0 {
		ctx.WritePlain("(")
		ctx.In.Write([]byte{'\n'})
		for i, col := range n.Cols {
			if err := col.Restore(ctx); err != nil {
				return errors.Annotatef(err, "An error occurred while splicing CreateTableStmt ColumnDef: [%v]", i)
			}
			ctx.In.Write([]byte(" " + genRange(col)))
			if i != lenCols-1 || lenConstraints > 0 {
				ctx.WritePlain(",")
				ctx.In.Write([]byte{'\n'})
			}
		}
		for i, constraint := range n.Constraints {
			if i > 0 {
				ctx.WritePlain(",")
				ctx.In.Write([]byte{'\n'})
			}
			if err := constraint.Restore(ctx); err != nil {
				return errors.Annotatef(err, "An error occurred while splicing CreateTableStmt Constraints: [%v]", i)
			}
		}
		ctx.In.Write([]byte{'\n'})
		ctx.WritePlain(")")
	}

	for i, option := range n.Options {
		ctx.WritePlain(" ")
		if err := option.Restore(ctx); err != nil {
			return errors.Annotatef(err, "An error occurred while splicing CreateTableStmt TableOption: [%v]", i)
		}
	}

	if n.Partition != nil {
		ctx.WritePlain(" ")
		if err := n.Partition.Restore(ctx); err != nil {
			return errors.Annotate(err, "An error occurred while splicing CreateTableStmt Partition")
		}
	}
	ctx.WritePlain(";\n")
	return nil
}

func genRange(col *ast.ColumnDef) string {
	if mysql.HasAutoIncrementFlag(col.Tp.Flag) || mysql.HasPriKeyFlag(col.Tp.Flag) {
		switch col.Tp.Tp {
		case mysql.TypeTimestamp, mysql.TypeDatetime:
			return "{{ timestamp '2000-01-01 00:00:00' + interval rownum second }}"
		case mysql.TypeDuration:
			return "{{ timestamp '00:00:00' + interval rownum second }}"
		case mysql.TypeDate:
			return "{{ timestamp '1000-01-01' + interval rownum second }}"
		case mysql.TypeVarchar, mysql.TypeBlob, mysql.TypeVarString, mysql.TypeString:
			if col.Tp.Flen < 8 {
				return fmt.Sprintf("{{ rand.regex('[0-9a-zA-Z]{1,%d}') }}", col.Tp.Flen)
			}
			fallthrough
		default:
			return "{{ rownum }}"
		}
	}

	defaultFlen, defaultDecimal := mysql.GetDefaultFieldLengthAndDecimal(col.Tp.Tp)
	flen := col.Tp.Flen
	if flen <= 0 {
		flen = defaultFlen
	}
	dec := col.Tp.Decimal
	if dec <= 0 {
		dec = defaultDecimal
	}

	switch col.Tp.Tp {
	case mysql.TypeDecimal:
		return unimplemented()
	case mysql.TypeTiny:
		max := math.Min(math.Pow10(flen)-1, 0x7f)
		return fmt.Sprintf("{{ rand.range_inclusive(0, %.0f) }}", max)
	case mysql.TypeShort:
		max := math.Min(math.Pow10(flen)-1, 0x7fff)
		return fmt.Sprintf("{{ rand.range_inclusive(0, %.0f) }}", max)
	case mysql.TypeLong:
		max := math.Min(math.Pow10(flen)-1, 0x7fffffff)
		return fmt.Sprintf("{{ rand.range_inclusive(0, %.0f) }}", max)
	case mysql.TypeFloat:
		return "{{ rand.finite_f32() }}"
	case mysql.TypeDouble:
		if dec < 0 {
			return "{{ rand.finite_f64() }}"
		} else {
			return fmt.Sprintf("{{ rand.regex('[0-9]{%d}\\.[0-9]{%d}') }}", flen-dec, dec)
		}
	case mysql.TypeNull:
		return "{{ NULL }}"
	case mysql.TypeTimestamp:
		return "{{ rand.u31_timestamp() }}"
	case mysql.TypeLonglong:
		max := math.Min(math.Pow10(flen)-1, 0x7ffffffffffff800) // avoid dealing with float -> int rounding
		return fmt.Sprintf("{{ rand.range_inclusive(0, %.0f) }}", max)
	case mysql.TypeInt24:
		max := math.Min(math.Pow10(flen)-1, 0x7fffff)
		return fmt.Sprintf("{{ rand.range_inclusive(0, %.0f) }}", max)
	case mysql.TypeDate:
		return "{{ TIMESTAMP '2016-01-02 15:04:05' }}"
	case mysql.TypeDuration:
		return "{{ TIMESTAMP '15:04:05' }}"
	case mysql.TypeDatetime:
		return "{{ rand.u31_timestamp() }}"
	case mysql.TypeYear:
		return "{{ rand.range(1970, 2200) }}"
	case mysql.TypeNewDate:
		return "{{ TIMESTAMP '2016-01-02' }}"
	case mysql.TypeBit:
		return `{{ rand.regex('[\u{0}\u{1}]') }}`
	case mysql.TypeNewDecimal:
		// TODO: parser bug
		if dec == 0 {
			flen = defaultFlen
		}
		return fmt.Sprintf("{{ rand.regex('[0-9]{%d}\\.[0-9]{%d}') }}", flen-dec, dec)
	case mysql.TypeEnum:
		return unimplemented()
	case mysql.TypeTinyBlob:
		return fmt.Sprintf("{{ rand.regex('[0-9a-zA-Z]{1,%d}') }}", sizeTinyBlob)
	case mysql.TypeMediumBlob:
		return fmt.Sprintf("{{ rand.regex('[0-9a-zA-Z]{1,%d}') }}", sizeMediumBlob)
	case mysql.TypeLongBlob:
		return fmt.Sprintf("{{ rand.regex('[0-9a-zA-Z]{1,%d}') }}", sizeLongBlob)
	case mysql.TypeVarchar, mysql.TypeBlob, mysql.TypeVarString, mysql.TypeString:
		return fmt.Sprintf("{{ rand.regex('[0-9a-zA-Z]{1,%d}') }}", flen)
	default:
		return unimplemented()
	}
}

func unimplemented() string {
	panic("unimplemented")
}
