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

// UploadByFilename 更新文件
func (client *FdfsClient) UploadByFilename(filename string) (*UploadFileResponse, error) {
	if err := fdfsCheckFile(filename); err != nil {
		return nil, errors.New(err.Error() + "(uploading)")
	}

	tc := &TrackerClient{client.trackerPool}
	storeServ, err := tc.trackerQueryStorageStorWithoutGroup()
	if err != nil {
		return nil, err
	}

	storagePool, err := client.getStoragePool(storeServ.ipAddr, storeServ.port)
	if err != nil {
		return nil, err
	}

	store := &StorageClient{storagePool}
	return store.storageUploadByFilename(tc, storeServ, filename)
}

// UploadByBuffer 更新数据
func (client *FdfsClient) UploadByBuffer(filebuffer []byte, fileExtName string) (*UploadFileResponse, error) {
	tc := &TrackerClient{client.trackerPool}
	storeServ, err := tc.trackerQueryStorageStorWithoutGroup()
	if err != nil {
		return nil, err
	}

	storagePool, err := client.getStoragePool(storeServ.ipAddr, storeServ.port)
	if err != nil {
		return nil, err
	}
	store := &StorageClient{storagePool}
	return store.storageUploadByBuffer(tc, storeServ, filebuffer, fileExtName)
}

// UploadSlaveByFilename 更新从文件
func (client *FdfsClient) UploadSlaveByFilename(filename, remoteFileID, prefixName string) (*UploadFileResponse, error) {
	if err := fdfsCheckFile(filename); err != nil {
		return nil, errors.New(err.Error() + "(uploading)")
	}

	tmp, err := splitRemoteFileID(remoteFileID)
	if err != nil || len(tmp) != 2 {
		return nil, err
	}
	groupName := tmp[0]
	remoteFilename := tmp[1]

	tc := &TrackerClient{client.trackerPool}
	storeServ, err := tc.trackerQueryStorageStorWithGroup(groupName)
	if err != nil {
		return nil, err
	}

	storagePool, err := client.getStoragePool(storeServ.ipAddr, storeServ.port)
	if err != nil {
		return nil, err
	}

	store := &StorageClient{storagePool}
	return store.storageUploadSlaveByFilename(tc, storeServ, filename, prefixName, remoteFilename)
}

// UploadSlaveByBuffer 更新从数据
func (client *FdfsClient) UploadSlaveByBuffer(filebuffer []byte, remoteFileID, fileExtName string) (*UploadFileResponse, error) {
	tmp, err := splitRemoteFileID(remoteFileID)
	if err != nil || len(tmp) != 2 {
		return nil, err
	}
	groupName := tmp[0]
	remoteFilename := tmp[1]

	tc := &TrackerClient{client.trackerPool}
	storeServ, err := tc.trackerQueryStorageStorWithGroup(groupName)
	if err != nil {
		return nil, err
	}

	storagePool, err := client.getStoragePool(storeServ.ipAddr, storeServ.port)
	if err != nil {
		return nil, err
	}

	store := &StorageClient{storagePool}
	return store.storageUploadSlaveByBuffer(tc, storeServ, filebuffer, remoteFilename, fileExtName)
}

// UploadAppenderByFilename 追加数据
func (client *FdfsClient) UploadAppenderByFilename(filename string) (*UploadFileResponse, error) {
	if err := fdfsCheckFile(filename); err != nil {
		return nil, errors.New(err.Error() + "(uploading)")
	}

	tc := &TrackerClient{client.trackerPool}
	storeServ, err := tc.trackerQueryStorageStorWithoutGroup()
	if err != nil {
		return nil, err
	}

	storagePool, err := client.getStoragePool(storeServ.ipAddr, storeServ.port)
	if err != nil {
		return nil, err
	}

	store := &StorageClient{storagePool}
	return store.storageUploadAppenderByFilename(tc, storeServ, filename)
}

// UploadAppenderByBuffer 追加数据
func (client *FdfsClient) UploadAppenderByBuffer(filebuffer []byte, fileExtName string) (*UploadFileResponse, error) {
	tc := &TrackerClient{client.trackerPool}
	storeServ, err := tc.trackerQueryStorageStorWithoutGroup()
	if err != nil {
		return nil, err
	}

	storagePool, err := client.getStoragePool(storeServ.ipAddr, storeServ.port)
	if err != nil {
		return nil, err
	}

	store := &StorageClient{storagePool}
	return store.storageUploadAppenderByBuffer(tc, storeServ, filebuffer, fileExtName)
}

// DeleteFile 删除文件
func (client *FdfsClient) DeleteFile(remoteFileID string) error {
	tmp, err := splitRemoteFileID(remoteFileID)
	if err != nil || len(tmp) != 2 {
		return err
	}
	groupName := tmp[0]
	remoteFilename := tmp[1]

	tc := &TrackerClient{client.trackerPool}
	storeServ, err := tc.trackerQueryStorageUpdate(groupName, remoteFilename)
	if err != nil {
		return err
	}

	storagePool, err := client.getStoragePool(storeServ.ipAddr, storeServ.port)
	if err != nil {
		return err
	}

	store := &StorageClient{storagePool}
	return store.storageDeleteFile(tc, storeServ, remoteFilename)
}

// DownloadToFile 下载文件
func (client *FdfsClient) DownloadToFile(localFilename string, remoteFileID string, offset int64, downloadSize int64) (*DownloadFileResponse, error) {
	tmp, err := splitRemoteFileID(remoteFileID)
	if err != nil || len(tmp) != 2 {
		return nil, err
	}
	groupName := tmp[0]
	remoteFilename := tmp[1]

	tc := &TrackerClient{client.trackerPool}
	storeServ, err := tc.trackerQueryStorageFetch(groupName, remoteFilename)
	if err != nil {
		return nil, err
	}

	storagePool, err := client.getStoragePool(storeServ.ipAddr, storeServ.port)
	if err != nil {
		return nil, err
	}

	store := &StorageClient{storagePool}
	return store.storageDownloadToFile(tc, storeServ, localFilename, offset, downloadSize, remoteFilename)
}

// DownloadToBuffer 下载文件
func (client *FdfsClient) DownloadToBuffer(remoteFileID string, offset int64, downloadSize int64) (*DownloadFileResponse, error) {
	tmp, err := splitRemoteFileID(remoteFileID)
	if err != nil || len(tmp) != 2 {
		return nil, err
	}
	groupName := tmp[0]
	remoteFilename := tmp[1]

	tc := &TrackerClient{client.trackerPool}
	storeServ, err := tc.trackerQueryStorageFetch(groupName, remoteFilename)
	if err != nil {
		return nil, err
	}

	storagePool, err := client.getStoragePool(storeServ.ipAddr, storeServ.port)
	if err != nil {
		return nil, err
	}

	store := &StorageClient{storagePool}
	var fileBuffer []byte
	return store.storageDownloadToBuffer(tc, storeServ, fileBuffer, offset, downloadSize, remoteFilename)
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
