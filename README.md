# PHP 多进程 Master-Worker

- 使用多进程
- 支持Worker错误重试，仅仅实现业务即可
- 任务累积过多，自动Fork Worker进程
- 常驻 Worker 进程，减少进程 Fork 开销
- 非常驻 Worker 进程闲置，自动退出回收
- 支持日志

Demo: 基于Redis生产消费队列 在 test 目录中