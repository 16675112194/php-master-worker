<?php


require "../src/MasterWorker.php";


class RedisProducterConsumer extends MasterWorker
{

    const QUERY_NAME = 'query_name';
    
    /**
     * @var Redis
     */
    protected $redis = null;

    protected function workerBeforeExit()
    {
        $this->closeRedis();
        // 处理结束，把redis关闭
        $this->log('进程退出：' . posix_getpid());
    }

    /**
     * 得到队列长度
     */
    protected function getTaskLength()
    {
        return $this->getRedis()->lLen(static::QUERY_NAME);
    }

    /**
     * 出队
     * @return mixed
     */
    public function deQueue()
    {
        return $this->getRedis()->lPop(static::QUERY_NAME);
    }

    /**
     * 入队
     * @param $data
     * @return int
     */
    public function enQueue($data)
    {
        return $this->getRedis()->rPush(static::QUERY_NAME, $data);
    }

    /**
     * 消费的具体内容
     * 不要进行失败重试
     * 会自动进行
     * 如果失败直接抛出异常
     * @param $data
     */
    protected function consume($data)
    {
        // 错误抛出异常
        //throw new Exception('error:' . $data);
        $this->log(['consume' => $data]);
    }

    /**
     * @return Redis
     */
    protected function getRedis($force = false)
    {

        // 后续使用 predis 使用redis池
        if ($force || ! $this->redis) {
            $this->redis = new \Redis();
            $this->redis->connect('127.0.0.1', 6379, 2);
        }

        return $this->redis;
    }

    protected function closeRedis()
    {
        $this->redis && $this->redis->close();
        $this->redis = null;
        $this->log('redis 关闭');
    }

    protected function masterBeforeExit()
    {
        $this->log('master 进程退出：' . posix_getpid());
    }
}