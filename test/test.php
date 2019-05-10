<?php

require "./RedisProducterConsumer.php";

$producterConsumer = new RedisProducterConsumer();

// 清空任务队列
$producterConsumer->getRedis()->ltrim(RedisProducterConsumer::QUERY_NAME, 1, 0);

// 写入任务队列
for ($i = 1; $i <= 100; ++$i) {
    $producterConsumer->enQueue($i);
}

$producterConsumer->start();

// 查看运行的进程
// ps aux | grep test.php

// 试一试 Ctrl + C 在执行上面产看进程命令