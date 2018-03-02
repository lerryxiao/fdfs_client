package fdfs_client

import (
	"errors"
	"fmt"
	"io"
	"math/rand"
	"net"
	"os"
	"time"
)

var (
	// ErrClosed close错误
	ErrClosed = errors.New("pool is closed")
)

type pConn struct {
	net.Conn
	pool *ConnectionPool
}

func (c pConn) Close() error {
	return c.pool.put(c.Conn)
}

// ConnectionPool 连接池
type ConnectionPool struct {
	hosts    []string
	port     int
	minConns int
	maxConns int
	conns    chan net.Conn
}

// NewConnectionPool 新连接池
func NewConnectionPool(hosts []string, port int, minConns int, maxConns int) (*ConnectionPool, error) {
	if minConns < 0 || maxConns <= 0 || minConns > maxConns {
		return nil, errors.New("invalid conns settings")
	}
	cp := &ConnectionPool{
		hosts:    hosts,
		port:     port,
		minConns: minConns,
		maxConns: maxConns,
		conns:    make(chan net.Conn, maxConns),
	}
	for i := 0; i < minConns; i++ {
		conn, err := cp.makeConn()
		if err != nil {
			cp.Close()
			return nil, err
		}
		cp.conns <- conn
	}
	return cp, nil
}

// Get 获取
func (pool *ConnectionPool) Get() (net.Conn, error) {
	conns := pool.getConns()
	if conns == nil {
		return nil, ErrClosed
	}

	for {
		select {
		case conn := <-conns:
			if conn == nil {
				break
				//return nil, ErrClosed
			}
			if err := pool.activeConn(conn); err != nil {
				break
			}
			return pool.wrapConn(conn), nil
		default:
			if pool.Len() >= pool.maxConns {
				errmsg := fmt.Sprintf("Too many connctions %d", pool.Len())
				return nil, errors.New(errmsg)
			}
			conn, err := pool.makeConn()
			if err != nil {
				return nil, err
			}
			pool.conns <- conn
			//put connection to pool and go next `for` loop
			//return this.wrapConn(conn), nil
		}
	}

}

// Close 关闭
func (pool *ConnectionPool) Close() {
	conns := pool.conns
	pool.conns = nil

	if conns == nil {
		return
	}

	close(conns)

	for conn := range conns {
		conn.Close()
	}
}

// Len 长度
func (pool *ConnectionPool) Len() int {
	return len(pool.getConns())
}

func (pool *ConnectionPool) makeConn() (net.Conn, error) {
	host := pool.hosts[rand.Intn(len(pool.hosts))]
	addr := fmt.Sprintf("%s:%d", host, pool.port)
	return net.DialTimeout("tcp", addr, time.Minute)
}

func (pool *ConnectionPool) getConns() chan net.Conn {
	conns := pool.conns
	return conns
}

func (pool *ConnectionPool) put(conn net.Conn) error {
	if conn == nil {
		return errors.New("connection is nil")
	}
	if pool.conns == nil {
		return conn.Close()
	}

	select {
	case pool.conns <- conn:
		return nil
	default:
		return conn.Close()
	}
}

func (pool *ConnectionPool) wrapConn(conn net.Conn) net.Conn {
	c := pConn{pool: pool}
	c.Conn = conn
	return c
}

func (pool *ConnectionPool) activeConn(conn net.Conn) error {
	th := &trackerHeader{}
	th.cmd = FDFS_PROTO_CMD_ACTIVE_TEST
	th.sendHeader(conn)
	th.recvHeader(conn)
	if th.cmd == 100 && th.status == 0 {
		return nil
	}
	return errors.New("Conn unaliviable")
}

// TCPSendData tcp发送数据
func TCPSendData(conn net.Conn, bytesStream []byte) error {
	if _, err := conn.Write(bytesStream); err != nil {
		return err
	}
	return nil
}

// TCPSendFile tcp发送文件
func TCPSendFile(conn net.Conn, filename string) error {
	file, err := os.Open(filename)
	defer file.Close()
	if err != nil {
		return err
	}

	var fileSize int64
	if fileInfo, err := file.Stat(); err == nil {
		fileSize = fileInfo.Size()
	}

	if fileSize == 0 {
		errmsg := fmt.Sprintf("file size is zeor [%s]", filename)
		return errors.New(errmsg)
	}

	fileBuffer := make([]byte, fileSize)

	_, err = file.Read(fileBuffer)
	if err != nil {
		return err
	}
	return TCPSendData(conn, fileBuffer)
}

// TCPRecvResponse tcp接收数据
func TCPRecvResponse(conn net.Conn, bufferSize int64) ([]byte, int64, error) {
	recvBuff := make([]byte, 0, bufferSize)
	tmp := make([]byte, 256)
	var total int64
	for {
		n, err := conn.Read(tmp)
		total += int64(n)
		recvBuff = append(recvBuff, tmp[:n]...)
		if err != nil {
			if err != io.EOF {
				return nil, 0, err
			}
			break
		}
		if total == bufferSize {
			break
		}
	}
	return recvBuff, total, nil
}

// TCPRecvFile tcp接收文件
func TCPRecvFile(conn net.Conn, localFilename string, bufferSize int64) (int64, error) {
	file, err := os.Create(localFilename)
	defer file.Close()
	if err != nil {
		return 0, err
	}

	recvBuff, total, err := TCPRecvResponse(conn, bufferSize)
	if _, err := file.Write(recvBuff); err != nil {
		return 0, err
	}
	return total, nil
}
