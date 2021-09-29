package main

import (
	"context"
	"fmt"
	"github.com/jackc/pgx/v4"
	"github.com/jackc/pgx/v4/pgxpool"
	"log"
	"os"

	"github.com/jackc/pgtype"
)

/*
create type resolution as (
    width int,
    height int,
    scan char
);

create table foo (id int primary key, res resolution);
insert into foo values (1, (10, 10, 'P'));
insert into foo values (2, null);
insert into foo values (3, (-10, 10, 'P'));
insert into foo values (4, (10, 10, null));

select * from foo;
*/

// Resolution is a custom type defined in postgres.  We want to map it to
// a struct in Go.  Except... we might need to handle nulls.  In which case
// we'll go through a data transfer object (DTO).
type Resolution struct {
	Width, Height int
	Scan          rune
}

// This has nullable fields where deal with the database possibly returning
// null.  If you can guarantee the fields will not be null, then you don't
// need the DTO and you would just have the type above.
type resolutionDTO struct {
	Width, Height *int
	Scan          *rune
}

// AsResolution converts the DTO with its nulls into a semantically valid application type.
func (rdto resolutionDTO) AsResolution() Resolution {
	var result Resolution

	if rdto.Width == nil {
		result.Width = 0
	} else {
		result.Width = *rdto.Width
	}

	if rdto.Height == nil {
		result.Height = 0
	} else {
		result.Height = *rdto.Height
	}

	if rdto.Scan == nil {
		result.Scan = 'P'
	} else {
		result.Scan = *rdto.Scan
	}

	return result
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
		// This is a pointer to the DTO - because our value might be null, in
		// which case, `some` would be set to nil.  If you can guarantee the
		// fields in the resulting object will never be null, you can use a
		// pointer to the Resolution type instead of the DTO.
		var some *resolutionDTO
		if err := rows.Scan(&some); err != nil {
			log.Printf("Failed to scan: %v", err)
		} else {
			if some != nil {
				log.Printf("Got %v", some.AsResolution())
			} else {
				log.Printf("No defined resolution")
			}
		}
	}
}
