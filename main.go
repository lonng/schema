package main

import (
	"bytes"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"log"

	"github.com/pingcap/errors"
	"github.com/pingcap/parser"
	"github.com/pingcap/parser/ast"
	"github.com/pingcap/parser/format"
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
				genRange(col, ctx.In)
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

func genRange(col *ast.ColumnDef, writer io.Writer) {
	writer.Write([]byte("{{ rand.regex('[0-9a-z]{2}') }}\n"))
}