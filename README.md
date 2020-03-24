mist orderBook-rs

项目为mist的撮合模块rust版本，重写adex_engine
由于数据库撮合改为内存撮合，故把bridge的划转实时消息也要推到内存里
更新erc20的余额

orderStream(kafka)           bridgeStream(kafka)
    |                               |
    |                               |
进行挂单或者撮合更新余额（redis）    更新余额（redis）
    |
    |
   落表（postgresql）              
                 
                  