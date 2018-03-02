package fdfs_client

import (
	"bytes"
	"encoding/binary"
	"net"
)

// TrackerClient 追踪客户端
type TrackerClient struct {
	pool *ConnectionPool
}

func (client *TrackerClient) trackerQueryStorageStorWithoutGroup() (*StorageServer, error) {
	var (
		conn     net.Conn
		recvBuff []byte
		err      error
	)

	conn, err = client.pool.Get()
	if err != nil {
		return nil, err
	}
	defer conn.Close()

	th := &trackerHeader{}
	th.cmd = TRACKER_PROTO_CMD_SERVICE_QUERY_STORE_WITHOUT_GROUP_ONE
	th.sendHeader(conn)

	th.recvHeader(conn)
	if th.status != 0 {
		return nil, Errno{int(th.status)}
	}

	var (
		groupName      string
		ipAddr         string
		port           int64
		storePathIndex uint8
	)
	recvBuff, _, err = TCPRecvResponse(conn, th.pkgLen)
	if err != nil {
		return nil, err
	}
	buff := bytes.NewBuffer(recvBuff)
	// #recv_fmt |-group_name(16)-ipaddr(16-1)-port(8)-store_path_index(1)|
	groupName, err = readCstr(buff, FDFS_GROUP_NAME_MAX_LEN)
	ipAddr, err = readCstr(buff, IP_ADDRESS_SIZE-1)
	binary.Read(buff, binary.BigEndian, &port)
	binary.Read(buff, binary.BigEndian, &storePathIndex)
	return &StorageServer{ipAddr, int(port), groupName, int(storePathIndex)}, nil
}

func (client *TrackerClient) trackerQueryStorageStorWithGroup(groupName string) (*StorageServer, error) {
	var (
		conn     net.Conn
		recvBuff []byte
		err      error
	)

	conn, err = client.pool.Get()
	defer conn.Close()
	if err != nil {
		return nil, err
	}

	th := &trackerHeader{}
	th.cmd = TRACKER_PROTO_CMD_SERVICE_QUERY_STORE_WITH_GROUP_ONE
	th.pkgLen = int64(FDFS_GROUP_NAME_MAX_LEN)
	th.sendHeader(conn)

	groupBuffer := new(bytes.Buffer)
	// 16 bit groupName
	groupNameBytes := bytes.NewBufferString(groupName).Bytes()
	for i := 0; i < 16; i++ {
		if i >= len(groupNameBytes) {
			groupBuffer.WriteByte(byte(0))
		} else {
			groupBuffer.WriteByte(groupNameBytes[i])
		}
	}
	groupBytes := groupBuffer.Bytes()

	err = TCPSendData(conn, groupBytes)
	if err != nil {
		return nil, err
	}

	th.recvHeader(conn)
	if th.status != 0 {
		return nil, Errno{int(th.status)}
	}

	var (
		ipAddr         string
		port           int64
		storePathIndex uint8
	)
	recvBuff, _, err = TCPRecvResponse(conn, th.pkgLen)
	if err != nil {
		return nil, err
	}
	buff := bytes.NewBuffer(recvBuff)
	// #recv_fmt |-group_name(16)-ipaddr(16-1)-port(8)-store_path_index(1)|
	groupName, err = readCstr(buff, FDFS_GROUP_NAME_MAX_LEN)
	ipAddr, err = readCstr(buff, IP_ADDRESS_SIZE-1)
	binary.Read(buff, binary.BigEndian, &port)
	binary.Read(buff, binary.BigEndian, &storePathIndex)
	return &StorageServer{ipAddr, int(port), groupName, int(storePathIndex)}, nil
}

func (client *TrackerClient) trackerQueryStorageUpdate(groupName string, remoteFilename string) (*StorageServer, error) {
	return client.trackerQueryStorage(groupName, remoteFilename, TRACKER_PROTO_CMD_SERVICE_QUERY_UPDATE)
}

func (client *TrackerClient) trackerQueryStorageFetch(groupName string, remoteFilename string) (*StorageServer, error) {
	return client.trackerQueryStorage(groupName, remoteFilename, TRACKER_PROTO_CMD_SERVICE_QUERY_FETCH_ONE)
}

func (client *TrackerClient) trackerQueryStorage(groupName string, remoteFilename string, cmd int8) (*StorageServer, error) {
	var (
		conn     net.Conn
		recvBuff []byte
		err      error
	)

	conn, err = client.pool.Get()
	defer conn.Close()
	if err != nil {
		return nil, err
	}

	th := &trackerHeader{}
	th.pkgLen = int64(FDFS_GROUP_NAME_MAX_LEN + len(remoteFilename))
	th.cmd = cmd
	th.sendHeader(conn)

	// #query_fmt: |-group_name(16)-filename(file_name_len)-|
	queryBuffer := new(bytes.Buffer)
	// 16 bit groupName
	groupNameBytes := bytes.NewBufferString(groupName).Bytes()
	for i := 0; i < 16; i++ {
		if i >= len(groupNameBytes) {
			queryBuffer.WriteByte(byte(0))
		} else {
			queryBuffer.WriteByte(groupNameBytes[i])
		}
	}
	// remoteFilenameLen bit remoteFilename
	remoteFilenameBytes := bytes.NewBufferString(remoteFilename).Bytes()
	for i := 0; i < len(remoteFilenameBytes); i++ {
		queryBuffer.WriteByte(remoteFilenameBytes[i])
	}
	err = TCPSendData(conn, queryBuffer.Bytes())
	if err != nil {
		return nil, err
	}

	th.recvHeader(conn)
	if th.status != 0 {
		return nil, Errno{int(th.status)}
	}

	var (
		ipAddr         string
		port           int64
		storePathIndex uint8
	)
	recvBuff, _, err = TCPRecvResponse(conn, th.pkgLen)
	if err != nil {
		return nil, err
	}
	buff := bytes.NewBuffer(recvBuff)
	// #recv_fmt |-group_name(16)-ipaddr(16-1)-port(8)-store_path_index(1)|
	groupName, err = readCstr(buff, FDFS_GROUP_NAME_MAX_LEN)
	ipAddr, err = readCstr(buff, IP_ADDRESS_SIZE-1)
	binary.Read(buff, binary.BigEndian, &port)
	binary.Read(buff, binary.BigEndian, &storePathIndex)
	return &StorageServer{ipAddr, int(port), groupName, int(storePathIndex)}, nil
}
