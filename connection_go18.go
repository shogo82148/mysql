// +build go1.8

package mysql

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
)

// Ping implements driver.Pinger interface
func (mc *mysqlConn) Ping(ctx context.Context) error {
	if mc.isClosed() {
		errLog.Print(ErrInvalidConn)
		return driver.ErrBadConn
	}

	if err := mc.watchCancel(ctx); err != nil {
		return err
	}
	defer mc.finish()

	if err := mc.writeCommandPacket(comPing); err != nil {
		return err
	}
	if _, err := mc.readResultOK(); err != nil {
		return err
	}

	return nil
}

// BeginTx implements driver.ConnBeginTx interface
func (mc *mysqlConn) BeginTx(ctx context.Context, opts driver.TxOptions) (driver.Tx, error) {
	if sql.IsolationLevel(opts.Isolation) != sql.LevelDefault {
		return nil, errors.New("mysql: isolation levels not supported")
	}

	if err := mc.watchCancel(ctx); err != nil {
		return nil, err
	}

	var err error
	var tx driver.Tx
	if opts.ReadOnly {
		tx, err = mc.beginReadOnly()
	} else {
		tx, err = mc.Begin()
	}
	mc.finish()
	if err != nil {
		return nil, err
	}

	select {
	default:
	case <-ctx.Done():
		tx.Rollback()
		return nil, ctx.Err()
	}
	return tx, err
}

func (mc *mysqlConn) beginReadOnly() (driver.Tx, error) {
	if mc.isClosed() {
		errLog.Print(ErrInvalidConn)
		return nil, driver.ErrBadConn
	}
	// https://dev.mysql.com/doc/refman/5.7/en/innodb-performance-ro-txn.html
	err := mc.exec("START TRANSACTION READ ONLY")
	if err != nil {
		return nil, err
	}

	return &mysqlTx{mc}, nil
}

func (mc *mysqlConn) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	if err := mc.watchCancel(ctx); err != nil {
		return nil, err
	}
	defer mc.finish()

	dargs, err := namedValueToValue(args)
	if err != nil {
		return nil, err
	}
	return mc.Query(query, dargs)
}

func (mc *mysqlConn) ExecContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	if err := mc.watchCancel(ctx); err != nil {
		return nil, err
	}
	defer mc.finish()

	dargs, err := namedValueToValue(args)
	if err != nil {
		return nil, err
	}
	return mc.Exec(query, dargs)
}

func (mc *mysqlConn) PrepareContext(ctx context.Context, query string) (driver.Stmt, error) {
	if err := mc.watchCancel(ctx); err != nil {
		return nil, err
	}

	stmt, err := mc.Prepare(query)
	mc.finish()
	if err != nil {
		return nil, err
	}

	select {
	default:
	case <-ctx.Done():
		stmt.Close()
		return nil, ctx.Err()
	}
	return stmt, nil
}

func (stmt *mysqlStmt) QueryContext(ctx context.Context, args []driver.NamedValue) (driver.Rows, error) {
	if err := stmt.mc.watchCancel(ctx); err != nil {
		return nil, err
	}
	defer stmt.mc.finish()

	dargs, err := namedValueToValue(args)
	if err != nil {
		return nil, err
	}
	return stmt.Query(dargs)
}

func (stmt *mysqlStmt) ExecContext(ctx context.Context, args []driver.NamedValue) (driver.Result, error) {
	if err := stmt.mc.watchCancel(ctx); err != nil {
		return nil, err
	}
	defer stmt.mc.finish()

	dargs, err := namedValueToValue(args)
	if err != nil {
		return nil, err
	}
	return stmt.Exec(dargs)
}

func (mc *mysqlConn) watchCancel(ctx context.Context) error {
	done := ctx.Done()
	if done == context.Background().Done() {
		return nil
	}
	select {
	default:
	case <-done:
		return ctx.Err()
	}
	if mc.chCtx == nil {
		return nil
	}

	finished := make(chan struct{})
	mc.chCtx <- mysqlContext{
		ctx:      ctx,
		finished: finished,
	}
	mc.finished = finished
	return nil
}

func (mc *mysqlConn) startWatcher() {
	chCtx := make(chan mysqlContext, 1)
	mc.chCtx = chCtx
	go func() {
		for ctx := range chCtx {
			select {
			case <-ctx.ctx.Done():
				mc.cleanup()
			case <-ctx.finished:
			case <-mc.closed:
				return
			}
		}
	}()
}

func namedValueToValue(named []driver.NamedValue) ([]driver.Value, error) {
	dargs := make([]driver.Value, len(named))
	for n, param := range named {
		if len(param.Name) > 0 {
			return nil, errors.New("mysql: driver does not support the use of Named Parameters")
		}
		dargs[n] = param.Value
	}
	return dargs, nil
}
