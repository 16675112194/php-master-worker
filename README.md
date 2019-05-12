# PHP 多进程 Master-Worker

Master 进程为主进程，它维护了一个 Worker 进程队列、子任务队列和子结果集。Worker 进程队列中的 Worker 进程，不停地从任务队列中提取要处理的子任务，并将子任务的处理结果写入结果集。

- 使用多进程
- 支持Worker错误重试，仅仅实现业务即可
- 任务累积过多，自动Fork Worker进程
- 常驻 Worker 进程，减少进程 Fork 开销
- 临时 Worker 进程闲置，自动退出回收 - 临时工
- 支持日志
- 支持自定义消费错误回调处理

Demo: 基于Redis生产消费队列 在 test 目录中

![PHP](./docs/master-worker.png)

## 日志分析

- 3(可配置)个常驻的 Worker 进程进行消费，消费结束不会退出，会常驻运行

`permanent` 表示常驻进程, <23572> 表示当前进程ID

```
=Worker [permanent]==== [2019-05-10 05:50:58]  ===
<23572> : 消费中:68

=Worker [permanent]==== [2019-05-10 05:50:58]  ===
<23573> : 消费中:70

=Worker [permanent]==== [2019-05-10 05:50:59]  ===
<23572> : 消费结束:68; 剩余个数:

=Worker [permanent]==== [2019-05-10 05:50:59]  ===
<23573> : 消费结束:70; 剩余个数:50

=Worker [permanent]==== [2019-05-10 05:50:59]  ===
<23574> : 消费中:79

=Worker [permanent]==== [2019-05-10 05:51:00]  ===
<23574> : 消费结束:79; 剩余个数:37
```

## 临时 Worker 消费，空闲自动退出 - Worker进程数可伸缩

`temporary` 表示临时进程，空闲会自动退出

```
=Worker [temporary]==== [2019-05-10 05:50:59]  ===
<23576> : 消费中:73

=Worker [temporary]==== [2019-05-10 05:50:59]  ===
<23577> : 消费中:74

=Worker [temporary]==== [2019-05-10 05:51:00]  ===
<23576> : 消费结束:73; 剩余个数:38

=Worker [temporary]==== [2019-05-10 05:51:01]  ===
<23577> : 消费结束:74; 剩余个数:

=Worker [temporary]==== [2019-05-10 05:51:01]  ===
<23577> : 进程退出：23577

=Worker [temporary]==== [2019-05-10 05:51:05]  ===
<23576> : 进程退出：23576
```

## 3 次消费出现错误自动记录日志

```
=Worker [temporary]==== [2019-05-10 05:51:03]  ===
<23583> : array (
  'data' => NULL,
  'status' => 0,
  'errorMsg' => 'RedisException: read error on connection',
)
```

## 临时Worker进程空闲自动退出

配置最小 常驻 Worker数是3，最大进程数是 10

```
> ps aux | grep test.php | wc -l
      12
> ps aux | grep test.php | wc -l
      10
> ps aux | grep test.php | wc -l
       5
```

- 第一条：`grep test.php` 进程 + Master进程 + 10个Worker(3 个常驻，7个临时) = 12
- 第二条：`grep test.php` 进程 + Master进程 + 8个Worker(3 个常驻，5个临时) = 10; 两个空闲的临时 Worker 退出
- 第三条：`grep test.php` 进程 + Master进程 + 3个常驻Worker = 5; 所有空闲的临时 Worker 退出

## Ctrl + C 或者 kill Master进程ID

> 注意是`kill`不是`kill -9`，因为后者是无法捕获的信号，直接强制退出，这会导致Master无法通知子进程退出，Worker将一直运行

```
=Worker [permanent]==== [2019-05-12 05:32:07]  ===
<58543> : 进程退出：58543

=Worker [permanent]==== [2019-05-12 05:32:07]  ===
<58542> : 进程退出：58542

=Worker [permanent]==== [2019-05-12 05:32:07]  ===
<58544> : 进程退出：58544

=Master [permanent]==== [2019-05-12 05:32:07]  ===
<58541> : master 进程退出：58541
```

`kill Master进程ID`或者`Ctrl + C`后：

1. `Master`陆续通知`Worker`退出
2. 等待所有`Worker`结束，并且回收资源
3. `Master`自身退出