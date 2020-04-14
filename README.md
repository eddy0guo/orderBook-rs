mist orderBook-rs

 v1(废弃)
项目为mist的撮合模块rust版本，重写adex_engine
由于数据库撮合改为内存撮合，故把bridge的划转实时消息也要推到内存里
更新erc20的余额

orderStream(kafka)           bridgeStream(kafka)    flush_trade（撮合好的redis里的结果）
    |                               |                         |
    |                               |                         |
进行挂单或者撮合更新余额（redis）    更新余额（redis）           落表（postgresql） 


v2
1、bridge去掉
2、init定时同步订单信息到内存中，五分钟一次轮询？
3、数据结构设计，内存占用最小
4、深度全量更新放在redis中
5、先冻结资金，在落表，撮合后，更新数据，放款，记得操作资金
6、一个交易对一个引擎
7、redis缓存三类hash，localBlance，freeze，orderBook,都是增量更新，当然着三个hash也要有初始化（data_Process）


1、深度，初始化，增量推kafka给ws消费，trade也是这么推
2、冻结，这个上来就先全量冻结，watcher解冻
3、取消订单，先更新表然后cancle信息推kafka
4、撮合，数据结构，价格最优，时间靠前原则
5、落表，另外一个array放撮合好的结果trade，共享内存，异步的去落表更新数据


数据结构
id:number
price:number(available_amount)
amount:
created_at:

涉及的接口
1、取消订单的逻辑，从内存里去找然后，落表
2、计算冻结的逻辑，冻结在下单的时候就进行，而不是sql计算


                
                 
                  