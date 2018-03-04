package fdfsClient

import (
	"errors"
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/weilaihui/goconfig/config"
)

// Errno 错误
type Errno struct {
	status int
}

func (e Errno) Error() string {
	errmsg := fmt.Sprintf("errno [%d] ", e.status)
	switch e.status {
	case 17:
		errmsg += "File Exist"
	case 22:
		errmsg += "Argument Invlid"
	}
	return errmsg
}

// FdfsConfigParser 配置文件解析器
type FdfsConfigParser struct{}

var (
	// ConfigFile 配置文件
	ConfigFile *config.Config
)

func (parser *FdfsConfigParser) Read(filename string) (*config.Config, error) {
	return config.ReadDefault(filename)
}

func fdfsCheckFile(filename string) error {
	if _, err := os.Stat(filename); err != nil {
		return err
	}
	return nil
}

func readCstr(buff io.Reader, length int) (string, error) {
	str := make([]byte, length)
	n, err := buff.Read(str)
	if err != nil || n != len(str) {
		return "", Errno{255}
	}

	for i, v := range str {
		if v == 0 {
			str = str[0:i]
			break
		}
	}
	return string(str), nil
}
func getFileExt(filename string) string {
	parts := strings.Split(filename, ".")
	if len(parts) >= 2 {
		return parts[len(parts)-1]
	}
	return ""
}

func splitRemoteFileID(remoteFileID string) ([]string, error) {
	parts := strings.SplitN(remoteFileID, "/", 2)
	if len(parts) < 2 {
		return nil, errors.New("error remoteFileId")
	}
	return parts, nil
}
