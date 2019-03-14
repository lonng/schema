package main

import (
	"bytes"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"math"
	"os"

	"github.com/pingcap/errors"
	"github.com/pingcap/parser"
	"github.com/pingcap/parser/ast"
	"github.com/pingcap/parser/format"
	"github.com/pingcap/parser/mysql"
	_ "github.com/pingcap/tidb/types/parser_driver"
)

var cliArgs struct {
	ImportFilePath string
	OutputFilePath string
}

func main() {
	flag.StringVar(&cliArgs.ImportFilePath, "i", "", "mysql schema file")
	flag.StringVar(&cliArgs.OutputFilePath, "o", "", "template output file")
	flag.Parse()
	if cliArgs.ImportFilePath == "" {
		log.Fatal("mysql schema file should not empty")
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

	for _, stmt := range stmts {
		createTable, ok := stmt.(*ast.CreateTableStmt)
		if !ok {
			log.Printf("statement %T is not create table", stmt)
			continue
		}
		buf := &bytes.Buffer{}
		ctx := format.NewRestoreCtx(format.DefaultRestoreFlags, buf)
		err := restore(ctx, createTable)
		if err != nil {
			log.Printf("restore error: %v", err)
		}
		out.Write(buf.Bytes())
	}
}

func restore(ctx *format.RestoreCtx, n *ast.CreateTableStmt) error {
	ctx.WriteKeyWord("CREATE TABLE ")
	if n.IfNotExists {
		ctx.WriteKeyWord("IF NOT EXISTS ")
	}

	if err := n.Table.Restore(ctx); err != nil {
		return errors.Annotate(err, "An error occurred while splicing CreateTableStmt Table")
	}
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
	for _, opt := range col.Options {
		switch opt.Tp {
		case ast.ColumnOptionNotNull:
			col.Tp.Flag |= mysql.NotNullFlag
		case ast.ColumnOptionAutoIncrement:
			col.Tp.Flag |= mysql.AutoIncrementFlag
		}
	}
	if mysql.HasAutoIncrementFlag(col.Tp.Flag) {
		return "{{ rownum }}"
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
		return "{{ rand.finite_f64() }}"
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
		return "{{ INTERVAL 30 DAY }}"
	case mysql.TypeDatetime:
		return "{{ rand.u31_timestamp() }}"
	case mysql.TypeYear:
		return "{{ rand.range(1970, 2200) }}"
	case mysql.TypeNewDate:
		return "{{ TIMESTAMP '2016-01-02' }}"
	case mysql.TypeBit:
		return "{{ rand.range_inclusive(0, 1) }}"
	case mysql.TypeNewDecimal:
		return fmt.Sprintf("{{ rand.regex('[0-9]{%d}\\.[0-9]{%d}') }}", flen, dec)
	case mysql.TypeEnum:
		return unimplemented()
	case mysql.TypeTinyBlob:
		return "{{ rand.regex('[0-9a-zA-Z]{1,100}') }}"
	case mysql.TypeMediumBlob:
		return "{{ rand.regex('[0-9a-zA-Z]{1,200}') }}"
	case mysql.TypeLongBlob:
		return "{{ rand.regex('[0-9a-zA-Z]{1,300}') }}"
	case mysql.TypeVarchar, mysql.TypeBlob, mysql.TypeVarString, mysql.TypeString:
		return fmt.Sprintf("{{ rand.regex('[0-9a-zA-Z]{1,%d}') }}", flen)
	default:
		return unimplemented()
	}
}

func unimplemented() string {
	panic("unimplemented")
}
