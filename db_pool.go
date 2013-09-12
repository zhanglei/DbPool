package db_pool

import "database/sql"
import _ "github.com/go-sql-driver/mysql"

type DbPool struct {
    conns []*sql.DB
}

func (db_pool *DbPool) Close() {
    for _, conn := range db_pool.conns {
        conn.Close()
    }
}

func (db_pool *DbPool) Map(worker func(int, *sql.DB, chan *Request, chan *Result), result_chan chan *Result) chan *Request {
    request_chan := make(chan *Request, len(db_pool.conns))
    
    for i, conn := range db_pool.conns {
        go worker(i, conn, request_chan, result_chan)
    }
    
    return request_chan
}

func NewDbPool(engine, dial string, total_conns int) *DbPool {
    db_conns := make([]*sql.DB, 0, total_conns)
    
    for i := 0; i < total_conns; i++ {
        db_conn, err := sql.Open(engine, dial)
        
        if err != nil { 
            panic(err.Error()) 
        }
        
        db_conns = append(db_conns, db_conn)
    }
    
    return &DbPool{db_conns}
}