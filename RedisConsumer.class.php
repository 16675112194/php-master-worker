<?php
/**
 * Created by PhpStorm.
 * User: lizhicheng
 * Date: 2018/6/6
 * Time: 13:37
 */

abstract class RedisConsumer
{

    // 最稳妥流程：待处理队列 --> 正在处理的hash(就怕处理中突然进程停止) --> 处理完成从hash中移除
    // rpush入队 lpop出队
    protected $currentProcessNum = 0;
    protected $maxProcessNum = 3; // 最大3个进程
    protected $waitQueryMicTime = 100; // 等待100毫米
    protected $subProcessMaxLoopTimes = 50; // 连续这么多次队列为空就退出子进程
    protected $consumeTryTimes = 3; // 消费3次失败就放弃

    protected $logFile =  SYSDIR_LOG.'/webapp/'.GAME_NAME.'_redis_consumer.log';
    const QUERY_NAME = GAME_NAME.'_pay_call_back_query';
    /**
     * @var Redis
     */
    protected $redis = null;

    public function start()
    {
        while (true) {

            $pid = pcntl_fork();
            if ($pid == -1) {
                $msg = 'oppo pay consumer could not fork';
                $this->log($msg);
                die($msg);
            } elseif($pid) {
                // 父进程
                $this->currentProcessNum++;
                if ($this->currentProcessNum >= $this->maxProcessNum) {
                    $this->recycleProcess();
                }
            } else {
                // 子进程 消费
                $this->run();
                exit(0); // 子进程结束
            }
        }
    }

    // 回收子进程
    protected function recycleProcess()
    {
        $pid = pcntl_wait($status); //取得子进程结束状态
        $this->currentProcessNum--;
        if (!($exitStatus = pcntl_wifexited($status))) {
            $exitMessage =  sprintf('pay sub process: %d %s exited %d', $pid, $exitStatus, $status);
            $this->log($exitMessage);
        }
    }

    protected function log($msg)
    {
        try {
            $this->writeLog($msg, $this->logFile, '充值回调');
        } catch (\Exception $e) {
            _error($e->getMessage());
        }
    }

    /**
     * 得到队列长度
     */
    protected function getQueryLength()
    {
        return $this->getRedis()->lLen(static::QUERY_NAME);
    }

    /**
     * 子进程处理内容
     */
    protected function run()
    {
        $noDataLoopTime = 0;
        while ($noDataLoopTime <= $this->subProcessMaxLoopTimes) {
            try {
                $data = $this->deQueue();
                if ($data) {
                    $noDataLoopTime = 1; // 从新变从1开始
                    $this->consumeByRetry($data);
                } else {
                    ++$noDataLoopTime;
                }
            } catch (\Exception $e) {
                // 消费出现错误
                $this->log(['query' => $data, 'errorMsg' => $e->getMessage()]);
            }
        }
    }

    /**
     * @param $data
     * @param int $tryTimes
     * @throws Exception
     */
    protected function consumeByRetry($data, $tryTimes = 1)
    {
        $tryTimes = 1;
        $msg = '';
        // consume 返回false 为失败
        while ($tryTimes <= $this->consumeTryTimes) {
            try {
                if ($this->consume($data)) {
                    break; // 执行成功
                } else {
                    ++$tryTimes;
                    usleep($this->waitQueryMicTime);
                }
            } catch (\Exception $e) {
                $msg = $e->getMessage();
                ++$tryTimes;
            }
        }
        // 最后一次还报错
        if ($tryTimes > $this->consumeTryTimes) {
            throw new \Exception('【充值回调超过3次失败】服务端返回:'.$msg);
        }
    }

    /**
     * 出队
     * @return mixed
     */
    public function deQueue()
    {
        if ($data = $this->getRedis()->lPop(static::QUERY_NAME)) {
            return $this->decode($data);
        } else {
            return false;
        }
    }

    /**
     * 入队
     * @param $data
     * @return int
     */
    public function enQueue($data)
    {
        return $this->getRedis()->rPush(static::QUERY_NAME, $this->encode($data));
    }

    /**
     * 消费的具体内容
     * 不要进行失败重试
     * 会自动进行
     * 如果失败最好直接抛出异常
     * @param $data
     */
    abstract protected function consume($data);

    /**
     * @param $mixed
     * @param $filename
     * @param $header
     * @param bool $trace
     * @return bool
     * @throws exception
     */
    protected function writeLog($mixed, $filename, $header, $trace = false)
    {
        if (is_string($mixed)) {
            $text = $mixed;
        } else {
            $text = var_export($mixed, true);
        }
        $trace_list = "";
        if ($trace) {
            $_t = debug_backtrace();
            $trace_list = "-- TRACE : \r\n";
            foreach ($_t as $_line) {
                $trace_list .= "-- " . $_line ['file'] . "[" . $_line ['line'] . "] : " . $_line ['function'] . "()" . "\r\n";
            }
        }
        $text = "\r\n=" . $header . "==== " . strftime("[%Y-%m-%d %H:%M:%S] ") . " ===\r\n<" . getmypid() . "> : " . $text . "\r\n" . $trace_list;
        $h = fopen($filename, 'a');
        if (! $h) {
            throw new exception('Could not open logfile:' . $filename);
        }
        // exclusive lock, will get released when the file is closed
        if (! flock($h, LOCK_EX)) {
            return false;
        }
        if (fwrite($h, $text) === false) {
            throw new exception('Could not write to logfile:' . $filename);
        }
        flock($h, LOCK_UN);
        fclose($h);
        return true;
    }

    public function encode($data)
    {
        return serialize($data);
    }

    public function decode($data)
    {
        return unserialize($data);
    }

    /**
     * @return Redis
     */
    protected function getRedis()
    {
        if ($this->redis === null) {
            $this->redis = RedisDbClass::getInstance()->getConnection();
        }
        return $this->redis;
    }
}