package client

import (
	"errors"
	"fmt"
	"io"
	"net"
	"os"
)

// StorageClient 存储客户端
type StorageClient struct {
	pool *ConnectionPool
}

///////////////////////////////////////////////////////////////////////////////////////////////////
// upload
func (client *StorageClient) storageUploadByFilename(tc *TrackerClient,
	storeServ *StorageServer, filename string) (*UploadFileResponse, error) {
	fileInfo, err := os.Stat(filename)
	if err != nil {
		return nil, err
	}

	fileSize := fileInfo.Size()
	fileExtName := getFileExt(filename)

	return client.storageUploadFile(tc, storeServ, filename, int64(fileSize), FDFS_UPLOAD_BY_FILENAME,
		STORAGE_PROTO_CMD_UPLOAD_FILE, "", "", fileExtName)
}

func (client *StorageClient) storageUploadByBuffer(tc *TrackerClient,
	storeServ *StorageServer, fileBuffer []byte, fileExtName string) (*UploadFileResponse, error) {
	bufferSize := len(fileBuffer)

	return client.storageUploadFile(tc, storeServ, fileBuffer, int64(bufferSize), FDFS_UPLOAD_BY_BUFFER,
		STORAGE_PROTO_CMD_UPLOAD_FILE, "", "", fileExtName)
}

// ReadStream 读写stream
type ReadStream interface {
	io.ReaderAt
	io.Seeker
}

func (client *StorageClient) storageUploadByStream(tc *TrackerClient,
	storeServ *StorageServer, stream ReadStream, fileExtName string, size int64) (*UploadFileResponse, error) {
	if size <= 0 && stream != nil {
		_, _ = stream.Seek(0, io.SeekStart)
		size, _ = stream.Seek(0, io.SeekEnd)
		_, _ = stream.Seek(0, io.SeekStart)
	}
	return client.storageUploadFile(tc, storeServ, stream, size, FDFS_UPLOAD_BY_STREAM,
		STORAGE_PROTO_CMD_UPLOAD_FILE, "", "", fileExtName)
}

///////////////////////////////////////////////////////////////////////////////////////////////////
// upload slave
func (client *StorageClient) storageUploadSlaveByFilename(tc *TrackerClient,
	storeServ *StorageServer, filename string, prefixName string, remoteFileID string) (*UploadFileResponse, error) {
	fileInfo, err := os.Stat(filename)
	if err != nil {
		return nil, err
	}

	fileSize := fileInfo.Size()
	fileExtName := getFileExt(filename)

	return client.storageUploadFile(tc, storeServ, filename, int64(fileSize), FDFS_UPLOAD_BY_FILENAME,
		STORAGE_PROTO_CMD_UPLOAD_SLAVE_FILE, remoteFileID, prefixName, fileExtName)
}

func (client *StorageClient) storageUploadSlaveByBuffer(tc *TrackerClient,
	storeServ *StorageServer, fileBuffer []byte, remoteFileID string, fileExtName string) (*UploadFileResponse, error) {
	bufferSize := len(fileBuffer)

	return client.storageUploadFile(tc, storeServ, fileBuffer, int64(bufferSize), FDFS_UPLOAD_BY_BUFFER,
		STORAGE_PROTO_CMD_UPLOAD_SLAVE_FILE, "", remoteFileID, fileExtName)
}

func (client *StorageClient) storageUploadSlaveByStream(tc *TrackerClient,
	storeServ *StorageServer, stream ReadStream, remoteFileID string, fileExtName string, size int64) (*UploadFileResponse, error) {
	if size <= 0 && stream != nil {
		_, _ = stream.Seek(0, io.SeekStart)
		size, _ = stream.Seek(0, io.SeekEnd)
		_, _ = stream.Seek(0, io.SeekStart)
	}
	return client.storageUploadFile(tc, storeServ, stream, size, FDFS_UPLOAD_BY_STREAM,
		STORAGE_PROTO_CMD_UPLOAD_SLAVE_FILE, "", remoteFileID, fileExtName)
}

///////////////////////////////////////////////////////////////////////////////////////////////////
// upload append
func (client *StorageClient) storageUploadAppenderByFilename(tc *TrackerClient,
	storeServ *StorageServer, filename string) (*UploadFileResponse, error) {
	fileInfo, err := os.Stat(filename)
	if err != nil {
		return nil, err
	}

	fileSize := fileInfo.Size()
	fileExtName := getFileExt(filename)

	return client.storageUploadFile(tc, storeServ, filename, int64(fileSize), FDFS_UPLOAD_BY_FILENAME,
		STORAGE_PROTO_CMD_UPLOAD_APPENDER_FILE, "", "", fileExtName)
}

func (client *StorageClient) storageUploadAppenderByBuffer(tc *TrackerClient,
	storeServ *StorageServer, fileBuffer []byte, fileExtName string) (*UploadFileResponse, error) {
	bufferSize := len(fileBuffer)

	return client.storageUploadFile(tc, storeServ, fileBuffer, int64(bufferSize), FDFS_UPLOAD_BY_BUFFER,
		STORAGE_PROTO_CMD_UPLOAD_APPENDER_FILE, "", "", fileExtName)
}

func (client *StorageClient) storageUploadAppenderByStream(tc *TrackerClient,
	storeServ *StorageServer, stream ReadStream, fileExtName string, size int64) (*UploadFileResponse, error) {
	if size <= 0 && stream != nil {
		_, _ = stream.Seek(0, io.SeekStart)
		size, _ = stream.Seek(0, io.SeekEnd)
		_, _ = stream.Seek(0, io.SeekStart)
	}
	return client.storageUploadFile(tc, storeServ, stream, size, FDFS_UPLOAD_BY_STREAM,
		STORAGE_PROTO_CMD_UPLOAD_APPENDER_FILE, "", "", fileExtName)
}

///////////////////////////////////////////////////////////////////////////////////////////////////

func (client *StorageClient) storageUploadFile(tc *TrackerClient,
	storeServ *StorageServer, fileContent interface{}, fileSize int64, uploadType int,
	cmd int8, masterFilename string, prefixName string, fileExtName string) (*UploadFileResponse, error) {

	var (
		conn        net.Conn
		uploadSlave bool
		headerLen   int64 = 15
		reqBuf      []byte
		err         error
	)

	conn, err = client.pool.Get()
	if err != nil {
		return nil, err
	}

	defer func() {
		_ = conn.Close()
	}()

	masterFilenameLen := int64(len(masterFilename))
	if len(storeServ.groupName) > 0 && len(masterFilename) > 0 {
		uploadSlave = true
		// #slave_fmt |-master_len(8)-file_size(8)-prefix_name(16)-file_ext_name(6)
		//       #           -master_name(master_filename_len)-|
		headerLen = int64(38) + masterFilenameLen
	}

	th := &trackerHeader{}
	th.pkgLen = headerLen
	th.pkgLen += int64(fileSize)
	th.cmd = cmd
	th.sendHeader(conn)

	if uploadSlave {
		req := &uploadSlaveFileRequest{}
		req.masterFilenameLen = masterFilenameLen
		req.fileSize = int64(fileSize)
		req.prefixName = prefixName
		req.fileExtName = fileExtName
		req.masterFilename = masterFilename
		reqBuf, err = req.marshal()
	} else {
		req := &uploadFileRequest{}
		req.storePathIndex = uint8(storeServ.storePathIndex)
		req.fileSize = int64(fileSize)
		req.fileExtName = fileExtName
		reqBuf, err = req.marshal()
	}
	if err != nil {
		return nil, err
	}

	err = TCPSendData(conn, reqBuf)
	if err != nil {
		return nil, err
	}

	switch uploadType {
	case FDFS_UPLOAD_BY_FILENAME:
		{
			if filename, ok := fileContent.(string); ok {
				err = TCPSendFile(conn, filename)
			}
		}
	case FDFS_UPLOAD_BY_BUFFER:
		{
			if fileBuffer, ok := fileContent.([]byte); ok {
				err = TCPSendData(conn, fileBuffer)
			}
		}
	case FDFS_UPLOAD_BY_STREAM:
		{
			if fileStream, ok := fileContent.(ReadStream); ok == true && fileStream != nil {
				var (
					cahce   = make([]byte, 1024*1024*5) // 每次读写5m
					readPos int64
					readLen int
				)
				for readPos < fileSize {
					readLen, err = fileStream.ReadAt(cahce, readPos)
					if readLen > 0 {
						err = TCPSendData(conn, cahce[0:readLen])
						readPos += int64(readLen)
					}
					if err != nil && err != io.EOF {
						return nil, err
					}
				}
			}
		}
	}
	if err != nil {
		return nil, err
	}

	th.recvHeader(conn)
	if th.status != 0 {
		return nil, Errno{int(th.status)}
	}
	recvBuff, recvSize, err := TCPRecvResponse(conn, th.pkgLen)
	if recvSize <= int64(FDFS_GROUP_NAME_MAX_LEN) {
		errmsg := "[-] Error: Storage response length is not match, "
		errmsg += fmt.Sprintf("expect: %d, actual: %d", th.pkgLen, recvSize)
		return nil, errors.New(errmsg)
	}
	ur := &UploadFileResponse{}
	err = ur.unmarshal(recvBuff)
	if err != nil {
		errmsg := fmt.Sprintf("recvBuf can not unmarshal :%s", err.Error())
		return nil, errors.New(errmsg)
	}

	return ur, nil
}

func (client *StorageClient) storageDeleteFile(tc *TrackerClient, storeServ *StorageServer, remoteFilename string) error {
	var (
		conn   net.Conn
		reqBuf []byte
		err    error
	)

	conn, err = client.pool.Get()
	if err != nil {
		return err
	}

	defer func() {
		_ = conn.Close()
	}()

	th := &trackerHeader{}
	th.cmd = STORAGE_PROTO_CMD_DELETE_FILE
	fileNameLen := len(remoteFilename)
	th.pkgLen = int64(FDFS_GROUP_NAME_MAX_LEN + fileNameLen)
	th.sendHeader(conn)

	req := &deleteFileRequest{}
	req.groupName = storeServ.groupName
	req.remoteFilename = remoteFilename
	reqBuf, err = req.marshal()
	if err != nil {
		return err
	}

	err = TCPSendData(conn, reqBuf)
	if err != nil {
		return err
	}

	th.recvHeader(conn)
	if th.status != 0 {
		return Errno{int(th.status)}
	}
	return nil
}

func (client *StorageClient) storageDownloadToFile(tc *TrackerClient,
	storeServ *StorageServer, localFilename string, offset int64,
	downloadSize int64, remoteFilename string) (*DownloadFileResponse, error) {
	return client.storageDownloadFile(tc, storeServ, localFilename, offset, downloadSize, FDFS_DOWNLOAD_TO_FILE, remoteFilename)
}

func (client *StorageClient) storageDownloadToBuffer(tc *TrackerClient,
	storeServ *StorageServer, fileBuffer []byte, offset int64,
	downloadSize int64, remoteFilename string) (*DownloadFileResponse, error) {
	return client.storageDownloadFile(tc, storeServ, fileBuffer, offset, downloadSize, FDFS_DOWNLOAD_TO_BUFFER, remoteFilename)
}

func (client *StorageClient) storageDownloadFile(tc *TrackerClient,
	storeServ *StorageServer, fileContent interface{}, offset int64, downloadSize int64,
	downloadType int, remoteFilename string) (*DownloadFileResponse, error) {

	var (
		conn          net.Conn
		reqBuf        []byte
		localFilename string
		recvBuff      []byte
		recvSize      int64
		err           error
	)

	conn, err = client.pool.Get()
	if err != nil {
		return nil, err
	}

	defer func() {
		_ = conn.Close()
	}()

	th := &trackerHeader{}
	th.cmd = STORAGE_PROTO_CMD_DOWNLOAD_FILE
	th.pkgLen = int64(FDFS_PROTO_PKG_LEN_SIZE*2 + FDFS_GROUP_NAME_MAX_LEN + len(remoteFilename))
	th.sendHeader(conn)

	req := &downloadFileRequest{}
	req.offset = offset
	req.downloadSize = downloadSize
	req.groupName = storeServ.groupName
	req.remoteFilename = remoteFilename
	reqBuf, err = req.marshal()
	if err != nil {
		return nil, err
	}

	err = TCPSendData(conn, reqBuf)
	if err != nil {
		return nil, err
	}

	th.recvHeader(conn)
	if th.status != 0 {
		return nil, Errno{int(th.status)}
	}

	switch downloadType {
	case FDFS_DOWNLOAD_TO_FILE:
		if localFilename, ok := fileContent.(string); ok {
			recvSize, err = TCPRecvFile(conn, localFilename, th.pkgLen)
		}
	case FDFS_DOWNLOAD_TO_BUFFER:
		if _, ok := fileContent.([]byte); ok {
			recvBuff, recvSize, err = TCPRecvResponse(conn, th.pkgLen)
		}
	}
	if err != nil {
		return nil, err
	}
	if recvSize < downloadSize {
		errmsg := "[-] Error: Storage response length is not match, "
		errmsg += fmt.Sprintf("expect: %d, actual: %d", th.pkgLen, recvSize)
		return nil, errors.New(errmsg)
	}

	dr := &DownloadFileResponse{}
	dr.RemoteFileID = storeServ.groupName + string(os.PathSeparator) + remoteFilename
	if downloadType == FDFS_DOWNLOAD_TO_FILE {
		dr.Content = localFilename
	} else {
		dr.Content = recvBuff
	}
	dr.DownloadSize = recvSize
	return dr, nil
}
