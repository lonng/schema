package main

import (
	"bytes"
	"flag"
	"fmt"
	"io/ioutil"
	"log"

	"github.com/pingcap/errors"
	"github.com/pingcap/parser"
	"github.com/pingcap/parser/ast"
	"github.com/pingcap/parser/format"
	"github.com/pingcap/parser/mysql"
	_ "github.com/pingcap/tidb/types/parser_driver"
)

var cliArgs struct {
	ImportFilePath string
}

func main() {
	flag.StringVar(&cliArgs.ImportFilePath, "i", "", "mysql schema file")
	flag.Parse()
	if cliArgs.ImportFilePath == "" {
		log.Fatal("mysql schema file should not empty")
	}

	sql, err := ioutil.ReadFile(cliArgs.ImportFilePath)
	if err != nil {
		log.Fatalf("read schema file failed: %v", err)
	}

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
		fmt.Println(buf.String())
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
			if i > 0 {
				ctx.WritePlain(",")
				ctx.In.Write([]byte{'\n'})
				ctx.In.Write([]byte(genRange(col)))
				ctx.In.Write([]byte{'\n'})
			}
			if err := col.Restore(ctx); err != nil {
				return errors.Annotatef(err, "An error occurred while splicing CreateTableStmt ColumnDef: [%v]", i)
			}
		}
		for i, constraint := range n.Constraints {
			if i > 0 || lenCols >= 1 {
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
	ctx.WritePlain(";")
	return nil
}

func genRange(col *ast.ColumnDef) string {
	//map[1:1717 2:88 3:4837 4:1 5:33 7:2661 8:3286 10:229 11:11 12:749 15:8319 16:74 246:2149 250:2 251:47 252:241 254:228]
	switch col.Tp.Tp {
	case mysql.TypeDecimal:
		return "{{ rand.range(1, 0xFFFFFFFFFFFFFFFF)}}"
	case mysql.TypeTiny:
		return "{{ rand.range(1, 0xFF)}}"
	case mysql.TypeShort:
		return "{{ rand.range(1, 0xFFFF)}}"
	case mysql.TypeLong:
		return "{{ rand.range(1, 0xFFFFFFFF)}}"
	case mysql.TypeFloat:
		return "{{ rand.finite_f32() }}"
	case mysql.TypeDouble:
		return "{{ rand.finite_f64() }}"
	case mysql.TypeNull:
		return "{{ NULL }}"
	case mysql.TypeTimestamp:
		return "{{ rand.u31_timestamp() }}"
	case mysql.TypeLonglong:
		return "{{ rand.range(1, 0xFFFFFFFFFFFFFFFF)}}"
	case mysql.TypeInt24:
		return "{{ rand.range(1, 0xFFFFFF)}}"
	case mysql.TypeDate:
		return "{{ TIMESTAMP '2016-01-02' }}"
	case mysql.TypeDuration:
		return "{{ INTERVAL 30 DAY }}"
	case mysql.TypeDatetime:
		return "{{ rand.u31_timestamp() }}"
	case mysql.TypeYear:
		return "{{ rand.range(1970, 2200)}}"
	case mysql.TypeNewDate:
		return "{{ TIMESTAMP '2016-01-02' }}"
	case mysql.TypeBit:
		return "{{ rand.range_inclusive(0, 1)}}"
	case mysql.TypeNewDecimal:
		return "{{ rand.range(1, 0xFFFFFFFFFFFFFFFF)}}"
	case mysql.TypeEnum:
		return unimplemented()
	case mysql.TypeTinyBlob:
		return "{{ rand.regex('[0-9a-z]+', 'i', 100) "
	case mysql.TypeMediumBlob:
		return "{{ rand.regex('[0-9a-z]+', 'i', 32600) "
	case mysql.TypeLongBlob:
		return "{{ rand.regex('[0-9a-z]+', 'i', 1000000) "
	case mysql.TypeVarchar, mysql.TypeBlob, mysql.TypeVarString, mysql.TypeString:
		return fmt.Sprintf("{{ rand.regex('[0-9a-z]+', 'i', %d) }}", col.Tp.Flen)
	default:
		return unimplemented()
	}
}

func unimplemented() string {
	panic("unimplemented")
}
