package postgres

import (
	"context"
	"fmt"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type DBConfig struct {
	Host     string
	Port     string
	User     string
	Password string
	Db       string
	SslMode  string
}

// NewDB создает и возвращает новый пул подключений к базе данных
func NewDB(ctx context.Context, cfg *DBConfig) (*pgxpool.Pool, error) {
	connectionUrl := fmt.Sprintf("host=%s port=%s user=%s password=%s dbname=%s sslmode=%s",
		cfg.Host, cfg.Port, cfg.User, cfg.Password, cfg.Db, cfg.SslMode)

	config, err := pgxpool.ParseConfig(connectionUrl)
	if err != nil {
		return nil, fmt.Errorf("failed to parse config: %w", err)
	}

	pool, err := pgxpool.NewWithConfig(ctx, config)
	if err != nil {
		return nil, fmt.Errorf("failed to create connection pool: %w", err)
	}

	return pool, nil
}

// QueryStructs выполняет SQL-запрос и возвращает результат в виде слайса структур
func QueryStructs[T any](ctx context.Context, pool *pgxpool.Pool, sql string, args ...any) ([]T, error) {
	rows, err := pool.Query(ctx, sql, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	return pgx.CollectRows(rows, pgx.RowToStructByName[T])
}

// QuerySimple выполняет SQL-запрос и возвращает результат в виде слайса простых типов
func QuerySimple[T any](ctx context.Context, pool *pgxpool.Pool, sql string, args ...any) ([]T, error) {
	rows, err := pool.Query(ctx, sql, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	return pgx.CollectRows(rows, pgx.RowTo[T])
}

// QueryOne выполняет SQL-запрос и возвращает один результат (одну строку, один столбец)
func QueryOne[T any](ctx context.Context, pool *pgxpool.Pool, sql string, args ...any) (T, error) {
	var t T
	err := pool.QueryRow(ctx, sql, args...).Scan(&t)
	return t, err
}

// QueryOneStruct выполняет SQL-запрос и возвращает результат в виде одной структуры
func QueryOneStruct[T any](ctx context.Context, pool *pgxpool.Pool, sql string, args ...any) (T, error) {
	rows, err := pool.Query(ctx, sql, args...)
	if err != nil {
		return *new(T), err
	}
	defer rows.Close()

	return pgx.CollectOneRow(rows, pgx.RowToStructByName[T])
}

// Exec выполняет SQL-запрос на изменение данных (INSERT, UPDATE, DELETE)
func Exec(ctx context.Context, pool *pgxpool.Pool, sql string, args ...any) error {
	_, err := pool.Exec(ctx, sql, args...)
	return err
}

// Close закрывает пул подключений
func Close(pool *pgxpool.Pool) {
	if pool != nil {
		pool.Close()
	}
}
