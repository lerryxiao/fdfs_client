package fdfs_client

import (
	"fmt"
	"os"
	"testing"
)

var (
	uploadResponse *UploadFileResponse
)

func TestParserFdfsConfig(t *testing.T) {
	fc := &FdfsConfigParser{}
	c, err := fc.Read("client.conf")
	if err != nil {
		t.Error(err)
		return
	}
	v, _ := c.String("DEFAULT", "base_path")
	t.Log(v)
}
func TestNewFdfsClientByTracker(t *testing.T) {
	tracker := &Tracker{
		[]string{"10.0.1.32"},
		22122,
	}
	_, err := NewFdfsClientByTracker(tracker)
	if err != nil {
		t.Error(err)
	}
}

func TestUploadByFilename(t *testing.T) {
	fdfsClient, err := NewFdfsClient("client.conf")
	if err != nil {
		t.Errorf("New FdfsClient error %s", err.Error())
		return
	}

	uploadResponse, err = fdfsClient.UploadByFilename("client.conf")
	if err != nil {
		t.Errorf("UploadByfilename error %s", err.Error())
	}
	t.Log(uploadResponse.GroupName)
	t.Log(uploadResponse.RemoteFileId)
	fdfsClient.DeleteFile(uploadResponse.RemoteFileId)
}

func TestUploadByBuffer(t *testing.T) {
	fdfsClient, err := NewFdfsClient("client.conf")
	if err != nil {
		t.Errorf("New FdfsClient error %s", err.Error())
		return
	}

	file, err := os.Open("testfile") // For read access.
	if err != nil {
		t.Fatal(err)
	}

	var fileSize int64
	if fileInfo, err := file.Stat(); err == nil {
		fileSize = fileInfo.Size()
	}
	fileBuffer := make([]byte, fileSize)
	_, err = file.Read(fileBuffer)
	if err != nil {
		t.Fatal(err)
	}

	uploadResponse, err = fdfsClient.UploadByBuffer(fileBuffer, "txt")
	if err != nil {
		t.Errorf("TestUploadByBuffer error %s", err.Error())
	}

	t.Log(uploadResponse.GroupName)
	t.Log(uploadResponse.RemoteFileId)
	fdfsClient.DeleteFile(uploadResponse.RemoteFileId)
}

func TestUploadSlaveByFilename(t *testing.T) {
	fdfsClient, err := NewFdfsClient("client.conf")
	if err != nil {
		t.Errorf("New FdfsClient error %s", err.Error())
		return
	}

	uploadResponse, err = fdfsClient.UploadByFilename("client.conf")
	if err != nil {
		t.Errorf("UploadByfilename error %s", err.Error())
	}
	t.Log(uploadResponse.GroupName)
	t.Log(uploadResponse.RemoteFileId)

	masterFileID := uploadResponse.RemoteFileId
	uploadResponse, err = fdfsClient.UploadSlaveByFilename("testfile", masterFileID, "_test")
	if err != nil {
		t.Errorf("UploadByfilename error %s", err.Error())
	}
	t.Log(uploadResponse.GroupName)
	t.Log(uploadResponse.RemoteFileId)

	fdfsClient.DeleteFile(masterFileID)
	fdfsClient.DeleteFile(uploadResponse.RemoteFileId)
}

func TestDownloadToFile(t *testing.T) {
	fdfsClient, err := NewFdfsClient("client.conf")
	if err != nil {
		t.Errorf("New FdfsClient error %s", err.Error())
		return
	}

	uploadResponse, err = fdfsClient.UploadByFilename("client.conf")
	defer fdfsClient.DeleteFile(uploadResponse.RemoteFileId)
	if err != nil {
		t.Errorf("UploadByfilename error %s", err.Error())
	}
	t.Log(uploadResponse.GroupName)
	t.Log(uploadResponse.RemoteFileId)

	var (
		downloadResponse *DownloadFileResponse
		localFilename    string = "download.txt"
	)
	downloadResponse, err = fdfsClient.DownloadToFile(localFilename, uploadResponse.RemoteFileId, 0, 0)
	if err != nil {
		t.Errorf("DownloadToFile error %s", err.Error())
	}
	t.Log(downloadResponse.DownloadSize)
	t.Log(downloadResponse.RemoteFileId)
}

func TestDownloadToBuffer(t *testing.T) {
	fdfsClient, err := NewFdfsClient("client.conf")
	if err != nil {
		t.Errorf("New FdfsClient error %s", err.Error())
		return
	}

	uploadResponse, err = fdfsClient.UploadByFilename("client.conf")
	defer fdfsClient.DeleteFile(uploadResponse.RemoteFileId)
	if err != nil {
		t.Errorf("UploadByfilename error %s", err.Error())
	}
	t.Log(uploadResponse.GroupName)
	t.Log(uploadResponse.RemoteFileId)

	var (
		downloadResponse *DownloadFileResponse
	)
	downloadResponse, err = fdfsClient.DownloadToBuffer(uploadResponse.RemoteFileId, 0, 0)
	if err != nil {
		t.Errorf("DownloadToBuffer error %s", err.Error())
	}
	t.Log(downloadResponse.DownloadSize)
	t.Log(downloadResponse.RemoteFileId)
}

func BenchmarkUploadByBuffer(b *testing.B) {
	fdfsClient, err := NewFdfsClient("client.conf")
	if err != nil {
		fmt.Printf("New FdfsClient error %s", err.Error())
		return
	}
	file, err := os.Open("testfile") // For read access.
	if err != nil {
		fmt.Printf("%s", err.Error())
	}

	var fileSize int64
	if fileInfo, err := file.Stat(); err == nil {
		fileSize = fileInfo.Size()
	}
	fileBuffer := make([]byte, fileSize)
	_, err = file.Read(fileBuffer)
	if err != nil {
		fmt.Printf("%s", err.Error())
	}

	b.StopTimer()
	b.StartTimer()

	for i := 0; i < b.N; i++ {
		uploadResponse, err = fdfsClient.UploadByBuffer(fileBuffer, "txt")
		if err != nil {
			fmt.Printf("TestUploadByBuffer error %s", err.Error())
		}

		fdfsClient.DeleteFile(uploadResponse.RemoteFileId)
	}
}

func BenchmarkUploadByFilename(b *testing.B) {
	fdfsClient, err := NewFdfsClient("client.conf")
	if err != nil {
		fmt.Printf("New FdfsClient error %s", err.Error())
		return
	}

	b.StopTimer()
	b.StartTimer()

	for i := 0; i < b.N; i++ {
		uploadResponse, err = fdfsClient.UploadByFilename("client.conf")
		if err != nil {
			fmt.Printf("UploadByfilename error %s", err.Error())
		}
		err = fdfsClient.DeleteFile(uploadResponse.RemoteFileId)
		if err != nil {
			fmt.Printf("DeleteFile error %s", err.Error())
		}
	}
}

func BenchmarkDownloadToFile(b *testing.B) {
	fdfsClient, err := NewFdfsClient("client.conf")
	if err != nil {
		fmt.Printf("New FdfsClient error %s", err.Error())
		return
	}

	uploadResponse, err = fdfsClient.UploadByFilename("client.conf")
	defer fdfsClient.DeleteFile(uploadResponse.RemoteFileId)
	if err != nil {
		fmt.Printf("UploadByfilename error %s", err.Error())
	}

	b.StopTimer()
	b.StartTimer()

	localFilename := "download.txt"
	for i := 0; i < b.N; i++ {
		_, err = fdfsClient.DownloadToFile(localFilename, uploadResponse.RemoteFileId, 0, 0)
		if err != nil {
			fmt.Printf("DownloadToFile error %s", err.Error())
		}
	}
}
