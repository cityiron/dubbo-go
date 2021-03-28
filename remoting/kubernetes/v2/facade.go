package v2

import "github.com/apache/dubbo-go/common"

type ClientFacade interface {
	Client() *Client
	SetClient(*Client)
	common.Node
}
