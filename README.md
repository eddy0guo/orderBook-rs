## mist-engine-rs      

##init
```sql
insert into mist_orders2 select * from mist_orders where market_id='MT-CNYC' and available_amount>0;
```

## build
```
cargo build or cargo build --release    
```          

##usage       
```
 ./target/debug/mist-engine --market_id=MT-CNYC
```
##TODO
```
1、redis余额的更新（不用做了）
2、撮合可用余额检查（暂时不需要检查了）
3、ws远程推送增量数据（bull要改成kafka了），或者把原来的adex——engine单独推送bull
4、trade的通过锁来进行多线程消费数据
5、压力测试
6、订单取消
```

##FIXME
```
1、一些unwrap的实现更换更安全的错误处理
```



                
                 
                  