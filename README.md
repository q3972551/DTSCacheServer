# DTSCacheServer
用阿里云的DTS缓存mysql 数据到redis上
阿里云的rds 无法使用自定义函数来实现mysql高速缓存到redis上。
但是常常游戏业务中我们会需要实现   server 去取redis数据，然后将数据保存进入mysql 进行持久化。为了避免各个应用模块维护多个数据。因此利用阿里云DTS，
订阅数据库，再这里代码上实现高速缓存
