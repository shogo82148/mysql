// +build go1.8

package mysql

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"runtime"
)

// Ping implements driver.Pinger interface
func (mc *mysqlConn) Ping(ctx context.Context) error {
	if mc.isClosed() {
		errLog.Print(ErrInvalidConn)
		return driver.ErrBadConn
	}

	done, err := mc.watchCancel(ctx)
	if err != nil {
		return err
	}
	defer close(done)

	err = mc.writeCommandPacket(comPing)
	if err != nil {
		errLog.Print(err)
	} else {
		_, err = mc.readResultOK()
		if err != nil {
			errLog.Print(err)
		}
	}

	select {
	default:
	case <-ctx.Done():
		return ctx.Err()
	}
	return err
}

// BeginTx implements driver.ConnBeginTx interface
func (mc *mysqlConn) BeginTx(ctx context.Context, opts driver.TxOptions) (driver.Tx, error) {
	if sql.IsolationLevel(opts.Isolation) != sql.LevelDefault {
		return nil, errors.New("mysql: isolation levels not supported")
	}

	done, err := mc.watchCancel(ctx)
	if err != nil {
		return nil, err
	}
	defer close(done)

	var tx driver.Tx
	if opts.ReadOnly {
		tx, err = mc.beginReadOnly()
	} else {
		tx, err = mc.Begin()
	}

	select {
	default:
	case <-ctx.Done():
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
	done, err := mc.watchCancel(ctx)
	if err != nil {
		return nil, err
	}
	defer close(done)

	dargs, err := namedValueToValue(args)
	if err != nil {
		return nil, err
	}
	rows, err := mc.Query(query, dargs)

	select {
	default:
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	return rows, err
}

func (mc *mysqlConn) ExecContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	done, err := mc.watchCancel(ctx)
	if err != nil {
		return nil, err
	}
	defer close(done)

	dargs, err := namedValueToValue(args)
	if err != nil {
		return nil, err
	}
	ret, err := mc.Exec(query, dargs)

	select {
	default:
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	return ret, err
}

func (mc *mysqlConn) PrepareContext(ctx context.Context, query string) (driver.Stmt, error) {
	done, err := mc.watchCancel(ctx)
	if err != nil {
		return nil, err
	}
	defer close(done)

	stmt, err := mc.Prepare(query)

	select {
	default:
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	return stmt, nil
}

func (stmt *mysqlStmt) QueryContext(ctx context.Context, args []driver.NamedValue) (driver.Rows, error) {
	done, err := stmt.mc.watchCancel(ctx)
	if err != nil {
		return nil, err
	}
	defer close(done)

	dargs, err := namedValueToValue(args)
	if err != nil {
		return nil, err
	}
	rows, err := stmt.Query(dargs)

	select {
	default:
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	return rows, err
}

func (stmt *mysqlStmt) ExecContext(ctx context.Context, args []driver.NamedValue) (driver.Result, error) {
	done, err := stmt.mc.watchCancel(ctx)
	if err != nil {
		return nil, err
	}
	defer close(done)

	dargs, err := namedValueToValue(args)
	if err != nil {
		return nil, err
	}
	ret, err := stmt.Exec(dargs)

	select {
	default:
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	return ret, err
}

func (mc *mysqlConn) watchCancel(ctx context.Context) (chan<- struct{}, error) {
	select {
	default:
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	if mc.chCtx == nil {
		return make(chan struct{}), nil
	}

	done := make(chan struct{})
	chCtx := mysqlContext{
		ctx:  ctx,
		done: done,
	}
	select {
	default:
		errLog.Print(ErrInvalidConn)
		return nil, driver.ErrBadConn
	case mc.chCtx <- chCtx:
	}
	return done, nil
}

func (mc *mysqlConn) startWatcher() {
	chCtx := make(chan mysqlContext, runtime.GOMAXPROCS(0))
	mc.chCtx = chCtx
	go func() {
		for ctx := range chCtx {
			select {
			case <-ctx.ctx.Done():
				mc.cleanup()
			case <-ctx.done:
			case <-mc.closed:
				return
			}
		}
	}()
}

// TODO: support the use of Named Parameters
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
