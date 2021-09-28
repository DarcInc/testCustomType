package main

import (
	"context"
	"fmt"
	"log"
	"os"

	"github.com/jackc/pgtype"
	"github.com/jackc/pgx/v4"
	"github.com/jackc/pgx/v4/pgxpool"
)

/*
create type resolution as (
    width int,
    height int,
    scan char
);

create table foo (id int primary key, res resolution);
insert into foo values (1, (10, 10, 'P'));
select * from foo;
*/

// Resolution is a custom type defined in postgres.  We want to map it to
// a struct in Go.
type Resolution struct {
	Width, Height int
	Scan          rune
}

// String to produce a human readable resolution.
func (r Resolution) String() string {
	return fmt.Sprintf("[%d, %d] at %c", r.Width, r.Height, r.Scan)
}

func main() {
	DBURI := os.Getenv("DB_URI")

	// Step 1: Create pool configuration
	poolConfig, err := pgxpool.ParseConfig(DBURI)
	if err != nil {
		log.Fatalf("Failed to parse config: %v", err)
	}

	// Step 2: Set the function to register the type
	poolConfig.AfterConnect = func(ctx context.Context, conn *pgx.Conn) error {
		// We retrieve the OID for our custom type.
		var oid uint32
		row := conn.QueryRow(context.Background(), "select 'resolution'::regtype::oid")
		if err := row.Scan(&oid); err != nil {
			log.Printf("Failed to scan oid: %v", err)
			return err
		}

		// Create the custom type
		ctype, err := pgtype.NewCompositeType("resolution", []pgtype.CompositeTypeField{
			{"width", pgtype.Int4OID},
			{"height", pgtype.Int4OID},
			{"scan", pgtype.BPCharOID},
		}, conn.ConnInfo())
		if err != nil {
			log.Printf("Failed to register new type: %v", err)
			return err
		}

		// Register the custom type with our connection.
		conn.ConnInfo().RegisterDataType(pgtype.DataType{
			Value: ctype,
			Name:  ctype.TypeName(),
			OID:   oid,
		})

		return nil
	}

	// Step 3: Create the pool
	pool, err := pgxpool.ConnectConfig(context.Background(), poolConfig)
	if err != nil {
		log.Fatalf("Bailing - no database connection: %v", err)
	}
	defer pool.Close()

	// Step 4: Profit
	conn, err := pool.Acquire(context.Background())
	if err != nil {
		log.Fatalf("Failed to acquire a connection from the pool: %v", err)
	}
	defer conn.Release()

	rows, err := conn.Query(context.Background(), "SELECT res FROM foo")
	if err != nil {
		log.Fatalf("Bailing - query failed: %v", err)
	}
	defer rows.Close()

	for rows.Next() {
		var some Resolution
		if err := rows.Scan(&some); err != nil {
			log.Printf("Failed to scan: %v", err)
		} else {
			log.Printf("Got %v", some)
		}
	}
}
