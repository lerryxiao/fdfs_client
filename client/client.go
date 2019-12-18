package client

import (
	"errors"
	"fmt"
	"runtime"
	"strconv"
	"strings"

	"github.com/jslyzt/goconfig/config"
)

var (
	storagePoolChan      chan *storagePool
	storagePoolMap       map[string]*ConnectionPool
	fetchStoragePoolChan chan interface{}
	quit                 chan bool
)

// FdfsClient fastdfs客户端
type FdfsClient struct {
	tracker     *Tracker
	trackerPool *ConnectionPool
	timeout     int
}

// Tracker 追踪
type Tracker struct {
	HostList []string
	Port     int
}

type storagePool struct {
	storagePoolKey string
	hosts          []string
	port           int
	minConns       int
	maxConns       int
}

func initvar() {
	storagePoolChan = make(chan *storagePool, 1)
	storagePoolMap = make(map[string]*ConnectionPool)
	fetchStoragePoolChan = make(chan interface{}, 1)
}

func init() {
	initvar()
	runtime.GOMAXPROCS(runtime.NumCPU())
	go func() {
		// start a loop
		for {
			select {
			case spd := <-storagePoolChan:
				if sp, ok := storagePoolMap[spd.storagePoolKey]; ok {
					fetchStoragePoolChan <- sp
				} else {
					var (
						sp  *ConnectionPool
						err error
					)
					sp, err = NewConnectionPool(spd.hosts, spd.port, spd.minConns, spd.maxConns)
					if err != nil {
						fetchStoragePoolChan <- err
					} else {
						storagePoolMap[spd.storagePoolKey] = sp
						fetchStoragePoolChan <- sp
					}
				}
			case <-quit:
				break
			}
		}
	}()
}

// GetTrackerConf 解析 tacker
func GetTrackerConf(confPath, confData string) (*Tracker, error) {
	var cf *config.Config
	var err error
	fc := &FdfsConfigParser{}
	if len(confData) > 0 {
		cf, err = fc.ReadData(confData)
	} else {
		cf, err = fc.ReadFile(confPath)
	}
	if err != nil {
		return nil, err
	}
	if cf == nil {
		return nil, nil
	}

	trackerListString, _ := cf.RawString("DEFAULT", "tracker_server")
	trackerList := strings.Split(trackerListString, ",")

	trackerIPList := make([]string, 0)
	trackerPort := "22122"

	for _, tr := range trackerList {
		tr = strings.TrimSpace(tr)
		parts := strings.Split(tr, ":")
		trackerIP := parts[0]
		if len(parts) == 2 {
			trackerPort = parts[1]
		}
		if trackerIP != "" {
			trackerIPList = append(trackerIPList, trackerIP)
		}
	}
	tp, err := strconv.Atoi(trackerPort)
	tracer := &Tracker{
		HostList: trackerIPList,
		Port:     tp,
	}
	return tracer, nil
}

// NewFdfsClient 新fastdfs客户端
func NewFdfsClient(confPath string) (*FdfsClient, error) {
	tracker, err := GetTrackerConf(confPath, "")
	if err != nil {
		return nil, err
	}

	trackerPool, err := NewConnectionPool(tracker.HostList, tracker.Port, 10, 150)
	if err != nil {
		return nil, err
	}

	return &FdfsClient{tracker: tracker, trackerPool: trackerPool}, nil
}

// NewFdfsClientByTracker 新fastdfs客户端
func NewFdfsClientByTracker(tracker *Tracker) (*FdfsClient, error) {
	trackerPool, err := NewConnectionPool(tracker.HostList, tracker.Port, 10, 150)
	if err != nil {
		return nil, err
	}

	return &FdfsClient{tracker: tracker, trackerPool: trackerPool}, nil
}

// ColseFdfsClient 关闭客户端
func ColseFdfsClient() {
	quit <- true
}

func (client *FdfsClient) getUploadArg(gname ...string) (tc *TrackerClient, srv *StorageServer, store *StorageClient, err error){
	tc = &TrackerClient{client.trackerPool}
	if len(gname) <= 0 {
		srv, err = tc.trackerQueryStorageStorWithoutGroup()
	} else {
		srv, err = tc.trackerQueryStorageStorWithGroup(gname[0])
	}
	if err != nil {
		return
	}
	var storagePool *ConnectionPool
	storagePool, err = client.getStoragePool(srv.ipAddr, srv.port)
	if err != nil {
		return
	}
	store = &StorageClient{storagePool}
	return
}

// UploadByFilename 上传文件
func (client *FdfsClient) UploadByFilename(filename string) (*UploadFileResponse, error) {
	if err := fdfsCheckFile(filename); err != nil {
		return nil, errors.New(err.Error() + "(uploading)")
	}
	tc, srv, store, err := client.getUploadArg()
	if err != nil {
		return nil, err
	}
	return store.storageUploadByFilename(tc, srv, filename)
}

// UploadByBuffer 上传数据
func (client *FdfsClient) UploadByBuffer(filebuffer []byte, fileExtName string) (*UploadFileResponse, error) {
	tc, srv, store, err := client.getUploadArg()
	if err != nil {
		return nil, err
	}
	return store.storageUploadByBuffer(tc, srv, filebuffer, fileExtName)
}

// UploadByStream 上传流
func (client *FdfsClient) UploadByStream(stream ReadStream, size int64, fileExtName string) (*UploadFileResponse, error) {
	tc, srv, store, err := client.getUploadArg()
	if err != nil {
		return nil, err
	}
	return store.storageUploadByStream(tc, srv, stream, fileExtName, size)
}

// UploadSlaveByFilename 上传从文件
func (client *FdfsClient) UploadSlaveByFilename(filename, remoteFileID, prefixName string) (*UploadFileResponse, error) {
	if err := fdfsCheckFile(filename); err != nil {
		return nil, errors.New(err.Error() + "(uploading)")
	}
	tmp, err := splitRemoteFileID(remoteFileID)
	if err != nil || len(tmp) != 2 {
		return nil, err
	}
	tc, srv, store, err := client.getUploadArg(tmp[0])
	if err != nil {
		return nil, err
	}
	return store.storageUploadSlaveByFilename(tc, srv, filename, prefixName, tmp[1])
}

// UploadSlaveByBuffer 上传从数据
func (client *FdfsClient) UploadSlaveByBuffer(filebuffer []byte, remoteFileID, fileExtName string) (*UploadFileResponse, error) {
	tmp, err := splitRemoteFileID(remoteFileID)
	if err != nil || len(tmp) != 2 {
		return nil, err
	}
	tc, srv, store, err := client.getUploadArg(tmp[0])
	if err != nil {
		return nil, err
	}
	return store.storageUploadSlaveByBuffer(tc, srv, filebuffer, tmp[1], fileExtName)
}

// UploadSlaveByStream 上传从流
func (client *FdfsClient) UploadSlaveByStream(stream ReadStream, size int64, remoteFileID, fileExtName string) (*UploadFileResponse, error) {
	tmp, err := splitRemoteFileID(remoteFileID)
	if err != nil || len(tmp) != 2 {
		return nil, err
	}
	tc, srv, store, err := client.getUploadArg(tmp[0])
	if err != nil {
		return nil, err
	}
	return store.storageUploadSlaveByStream(tc, srv, stream, tmp[1], fileExtName, size)
}

// UploadAppenderByFilename 追加文件
func (client *FdfsClient) UploadAppenderByFilename(filename string) (*UploadFileResponse, error) {
	if err := fdfsCheckFile(filename); err != nil {
		return nil, errors.New(err.Error() + "(uploading)")
	}
	tc, srv, store, err := client.getUploadArg()
	if err != nil {
		return nil, err
	}
	return store.storageUploadAppenderByFilename(tc, srv, filename)
}

// UploadAppenderByBuffer 追加数据
func (client *FdfsClient) UploadAppenderByBuffer(filebuffer []byte, fileExtName string) (*UploadFileResponse, error) {
	tc, srv, store, err := client.getUploadArg()
	if err != nil {
		return nil, err
	}
	return store.storageUploadAppenderByBuffer(tc, srv, filebuffer, fileExtName)
}

// UploadAppenderByStream 追加流
func (client *FdfsClient) UploadAppenderByStream(stream ReadStream, size int64, fileExtName string) (*UploadFileResponse, error) {
	tc, srv, store, err := client.getUploadArg()
	if err != nil {
		return nil, err
	}
	return store.storageUploadAppenderByStream(tc, srv, stream, fileExtName, size)
}

// DeleteFile 删除文件
func (client *FdfsClient) DeleteFile(remoteFileID string) error {
	tmp, err := splitRemoteFileID(remoteFileID)
	if err != nil || len(tmp) != 2 {
		return err
	}
	tc, srv, store, err := client.getUploadArg(tmp[0])
	if err != nil {
		return err
	}
	return store.storageDeleteFile(tc, srv, tmp[1])
}

// DownloadToFile 下载文件
func (client *FdfsClient) DownloadToFile(localFilename string, remoteFileID string, offset int64, downloadSize int64) (*DownloadFileResponse, error) {
	tmp, err := splitRemoteFileID(remoteFileID)
	if err != nil || len(tmp) != 2 {
		return nil, err
	}
	tc, srv, store, err := client.getUploadArg(tmp[0])
	if err != nil {
		return nil, err
	}
	return store.storageDownloadToFile(tc, srv, localFilename, offset, downloadSize, tmp[1])
}

// DownloadToBuffer 下载文件
func (client *FdfsClient) DownloadToBuffer(remoteFileID string, offset int64, downloadSize int64) (*DownloadFileResponse, error) {
	tmp, err := splitRemoteFileID(remoteFileID)
	if err != nil || len(tmp) != 2 {
		return nil, err
	}
	tc, srv, store, err := client.getUploadArg(tmp[0])
	if err != nil {
		return nil, err
	}
	var fileBuffer []byte
	return store.storageDownloadToBuffer(tc, srv, fileBuffer, offset, downloadSize, tmp[1])
}

func (client *FdfsClient) getStoragePool(ipAddr string, port int) (*ConnectionPool, error) {
	hosts := []string{ipAddr}
	storagePoolKey := fmt.Sprintf("%s-%d", ipAddr, port)
	var (
		result interface{}
		err    error
		ok     bool
	)

	spd := &storagePool{
		storagePoolKey: storagePoolKey,
		hosts:          hosts,
		port:           port,
		minConns:       10,
		maxConns:       150,
	}

	storagePoolChan <- spd
	for {
		select {
		case result = <-fetchStoragePoolChan:
			var storagePool *ConnectionPool
			if err, ok = result.(error); ok {
				return nil, err
			} else if storagePool, ok = result.(*ConnectionPool); ok {
				return storagePool, nil
			} else {
				return nil, errors.New("none")
			}
		}
	}
}
