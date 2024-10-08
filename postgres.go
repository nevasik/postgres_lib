package postgres

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"gitlab.com/nevasik7/lg"
	"strings"
	"time"
)

type DBConfig struct {
	Host        string
	Port        string
	User        string
	Password    string
	Db          string
	SslMode     string
	MaxConn     int
	MaxConnTime time.Duration
}

// NewDB создает и возвращает новый пул подключений к базе данных
func NewDB(ctx context.Context, cfg *DBConfig) (*pgxpool.Pool, error) {
	connectionUrl := fmt.Sprintf("host=%s port=%s user=%s password=%s dbname=%s sslmode=%s",
		cfg.Host, cfg.Port, cfg.User, cfg.Password, cfg.Db, cfg.SslMode)

	config, err := pgxpool.ParseConfig(connectionUrl)
	if err != nil {
		return nil, fmt.Errorf("failed to parse config: %w", err)
	}

	if cfg.MaxConn != 0 && cfg.MaxConnTime != 0 {
		config.MaxConns = int32(cfg.MaxConn)
		config.ConnConfig.ConnectTimeout = cfg.MaxConnTime
	}

	pool, err := pgxpool.NewWithConfig(ctx, config)
	if err != nil {
		return nil, fmt.Errorf("failed to create connection pool: %w", err)
	}

	return pool, nil
}

// QueryStructs выполняет SQL-запрос и возвращает результат в виде слайса структур
func QueryStructs[T any](ctx context.Context, pool *pgxpool.Pool, sql string, args ...any) ([]T, error) {
	start := time.Now()
	defer func() {
		elapsed := time.Since(start)
		lg.Infof("Executed %s in %s", sql, elapsed)
	}()

	rows, err := pool.Query(ctx, sql, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	return pgx.CollectRows(rows, pgx.RowToStructByName[T])
}

// QuerySimple выполняет SQL-запрос и возвращает результат в виде слайса простых типов
func QuerySimple[T any](ctx context.Context, pool *pgxpool.Pool, sql string, args ...any) ([]T, error) {
	start := time.Now()
	defer func() {
		elapsed := time.Since(start)
		lg.Infof("Executed %s in %s", sql, elapsed)
	}()

	rows, err := pool.Query(ctx, sql, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	return pgx.CollectRows(rows, pgx.RowTo[T])
}

// QueryOne выполняет SQL-запрос и возвращает один результат (одну строку, один столбец)
func QueryOne[T any](ctx context.Context, pool *pgxpool.Pool, sql string, args ...any) (T, error) {
	start := time.Now()
	defer func() {
		elapsed := time.Since(start)
		lg.Infof("Executed %s in %s", sql, elapsed)
	}()

	var t T
	err := pool.QueryRow(ctx, sql, args...).Scan(&t)

	return t, err
}

// QueryOneStruct выполняет SQL-запрос и возвращает результат в виде одной структуры
func QueryOneStruct[T any](ctx context.Context, pool *pgxpool.Pool, sql string, args ...any) (T, error) {
	start := time.Now()
	defer func() {
		elapsed := time.Since(start)
		lg.Infof("Executed %s in %s", sql, elapsed)
	}()

	rows, err := pool.Query(ctx, sql, args...)
	if err != nil {
		return *new(T), err
	}
	defer rows.Close()

	return pgx.CollectOneRow(rows, pgx.RowToStructByName[T])
}

// Exec выполняет SQL-запрос на изменение данных (INSERT, UPDATE, DELETE)
func Exec(ctx context.Context, pool *pgxpool.Pool, sql string, args ...any) error {
	start := time.Now()
	defer func() {
		elapsed := time.Since(start)
		lg.Infof("Executed %s in %s", sql, elapsed)
	}()

	_, err := pool.Exec(ctx, sql, args...)
	return err
}

// RequestInOneTransaction - Открывает новую транзакцию, в которую мы в виде map(k-запрос; v-массив аргументов) в пределах одной транзакции
func RequestInOneTransaction(ctx context.Context, pool *pgxpool.Pool, queryParam map[string][]any) error {
	start := time.Now()
	defer func() {
		elapsed := time.Since(start)
		lg.Infof("Executed requests is one tx in %s", elapsed)
	}()

	tx, err := beginTransaction(ctx, pool)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}

	for k, v := range queryParam {
		if _, err = tx.Exec(ctx, k, v); err != nil {
			_ = tx.Rollback(ctx)
			return fmt.Errorf("failed to execute query: %w", err)
		}
	}

	if err = tx.Commit(ctx); err != nil {
		return fmt.Errorf("failed to commit transaction: %w", err)
	}

	return nil
}

// BulkInsert выполняет пакетную вставку данных в указанную таблицу
func BulkInsert(ctx context.Context, pool *pgxpool.Pool, tableName string, columns []string, values [][]any) error {
	start := time.Now()
	defer func() {
		elapsed := time.Since(start)
		lg.Infof("Executed bulk insert to the%s in %s", tableName, elapsed)
	}()

	if len(values) == 0 {
		return fmt.Errorf("no values provided for insert")
	}

	valueStrings := make([]string, len(values))
	valueArgs := make([]any, 0, len(values)*len(columns))

	for i, row := range values {
		placeholders := make([]string, len(row))
		for j := range row {
			placeholders[j] = fmt.Sprintf("$%d", len(valueArgs)+j+1)
		}
		valueStrings[i] = fmt.Sprintf("(%s)", strings.Join(placeholders, ","))
		valueArgs = append(valueArgs, row...)
	}

	query := fmt.Sprintf(
		"INSERT INTO %s (%s) VALUES %s",
		tableName,
		strings.Join(columns, ","),
		strings.Join(valueStrings, ","),
	)

	_, err := pool.Exec(ctx, query, valueArgs...)
	if err != nil {
		return fmt.Errorf("bulk insert failed: %w", err)
	}

	return nil
}

// QueryJson выполняет запрос и возвращает результат в виде карты для полей JSONB
func QueryJson(ctx context.Context, pool *pgxpool.Pool, sql string, args ...any) (map[string]interface{}, error) {
	start := time.Now()
	defer func() {
		elapsed := time.Since(start)
		lg.Infof("Executed %s in %s", sql, elapsed)
	}()

	var result map[string]interface{}
	err := pool.QueryRow(ctx, sql, args...).Scan(&result)
	return result, err
}

// ExecJson для выполнения INSERT/UPDATE запросов с использованием JSONB
func ExecJson(ctx context.Context, pool *pgxpool.Pool, sql string, jsonData map[string]any, args ...any) error {
	start := time.Now()
	defer func() {
		elapsed := time.Since(start)
		lg.Infof("Executed %s in %s", sql, elapsed)
	}()

	jsonBytes, err := json.Marshal(jsonData)
	if err != nil {
		return err
	}

	_, err = pool.Exec(ctx, sql, append(args, jsonBytes)...)
	return err
}

// QueryWithPagination выполняет запрос с поддержкой пагинации
func QueryWithPagination[T any](ctx context.Context, pool *pgxpool.Pool, sql string, limit, offset int, args ...any) ([]T, error) {
	start := time.Now()
	defer func() {
		elapsed := time.Since(start)
		lg.Infof("Executed %s in %s", sql, elapsed)
	}()

	paginatedSQL := fmt.Sprintf("%s LIMIT $%d OFFSET $%d", sql, len(args)+1, len(args)+2)
	args = append(args, limit, offset)

	return QuerySimple[T](ctx, pool, paginatedSQL, args...)
}

// QueryWithCTE выполняет запрос с механизмом CTE(предварительная отсеивание неких данных)
func QueryWithCTE[T any](ctx context.Context, pool *pgxpool.Pool, cte string, query string, args ...any) ([]T, error) {
	start := time.Now()
	defer func() {
		elapsed := time.Since(start)
		lg.Infof("Executed CTE query in %s", elapsed)
	}()

	sql := fmt.Sprintf("WITH %s %s", cte, query)
	return QuerySimple[T](ctx, pool, sql, args...)
}

// Close закрывает пул подключений
func Close(pool *pgxpool.Pool) {
	if pool != nil {
		pool.Close()
	}
}
