package es

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"github.com/jeanphorn/log4go"
	"github.com/olivere/elastic/v7"
	"github.com/zlt-com/go-config"
)

// Clicent ElasitcSearch连接
var (
	EsClient *elastic.Client
	Enable   bool
)

// Start 启动
func Start() {
	Enable = config.Config.ElasticEnable
	if Enable {
		//初始化ElasticSearch
		var err error
		if EsClient, err = elastic.NewClient(
			elastic.SetURL(config.Config.ElasticHostURL),
			elastic.SetSniff(false),
			elastic.SetHealthcheckInterval(10*time.Second),
			elastic.SetGzip(true),
			elastic.SetBasicAuth(config.Config.ElasticUser, config.Config.ElasticPassword),
		); err != nil {
			log4go.LOGGER("err").Error(err)
			panic(err)
		}
	}
}

// Put 增加
func Put(i, t string, o interface{}) error {
	if EsClient == nil {
		return errors.New("EsClient not open,run Start() at init()")
	}
	ctx := context.Background()
	if _, err := EsClient.Index().Index(i).Type(t).BodyJson(o).Do(ctx); err != nil {
		return err
	}
	return nil
}

// Kvf Kvf
type Kvf struct {
	K, F string
	V    interface{}
}

// Get 获取
func Get(i string, size int, q elastic.Query) (objs []*json.RawMessage, err error) {
	index := EsClient.Search().Index(i).Query(q)
	if size > -1 {
		index = index.Size(size)
	}
	if searchResult, err := index.Pretty(true).Do(context.TODO()); err == nil {
		if searchResult.Hits != nil {
			objs = make([]*json.RawMessage, 0)
			// Iterate through results
			for _, hit := range searchResult.Hits.Hits {
				objs = append(objs, &hit.Source)
			}
		}
	}
	return
}

// Count Count
func Count(i string, q elastic.Query) (count int64) {
	//取所有
	if res, err := EsClient.Search(i).Query(q).Do(context.Background()); err == nil {
		count = res.Hits.TotalHits.Value
	}
	return
}
