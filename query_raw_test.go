package bun_test

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/uptrace/bun"
	"github.com/uptrace/bun/dbfixture"
	"github.com/uptrace/bun/dialect/pgdialect"
	"github.com/uptrace/bun/driver/pgdriver"
	"log"
	"os"
	"testing"
	"time"
)

func TestQueryRaw(t *testing.T) {
	type User struct {
		bun.BaseModel `bun:"table:users,alias:u"`

		ID        int64     `bun:",pk,autoincrement"`
		Username  string    `bun:",notnull"`
		Email     string    `bun:",notnull,unique"`
		Password  string    `bun:",notnull"`
		CreatedAt time.Time `bun:",nullzero,default:current_timestamp"`
	}

	type user struct {
		ID        int64
		UpdatedAt time.Time
	}

	dsn := "postgres://postgres:postgres@localhost:5433/postgres?sslmode=disable"
	sqldb := sql.OpenDB(pgdriver.NewConnector(pgdriver.WithDSN(dsn)))

	db := bun.NewDB(sqldb, pgdialect.New())
	db.RegisterModel((*User)(nil))

	t.Run("simple", func(t *testing.T) {
		var rows []*user

		ctx := context.Background()
		for i := 0; i < 100; i++ {
			fmt.Printf("Run number %d\n", i)
			fixture := dbfixture.New(db, dbfixture.WithTruncateTables())
			err := fixture.Load(ctx, os.DirFS("testdata"), "fixtures.yaml")
			if err != nil {
				log.Printf("failed to load fixtures: %v", err)
			}

			fmt.Println(fixture.MustRow("User.doe").(*User))

			err = db.NewRaw(
				"SELECT id, updated_at FROM (SELECT u1.id, u1.created_at as updated_at, ROW_NUMBER() over (PARTITION BY u1.id) as row_num FROM users AS u1 WHERE username is not null) sub WHERE row_num = 1",
				bun.Ident("users"), "username IS NOT NULL",
			).Scan(ctx, &rows)

			for _, r := range rows {
				fmt.Println(r)
			}
		}

	})

}
