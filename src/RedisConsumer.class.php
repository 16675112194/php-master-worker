<?php
declare(ticks = 1);
error_reporting(E_ERROR);
/**
 * Created by PhpStorm.
 * User: lizhicheng
 * Date: 2018/6/6
 * Time: 13:37
 */

class RedisConsumer
{

    // 最稳妥流程：待处理队列 --> 正在处理的hash(就怕处理中突然进程停止) --> 处理完成从hash中移除
    // rpush入队 lpop出队
    protected $maxProcessNum = 10; // 最大3个进程
    protected $minProcessNum = 3; // 最少进程
    protected $waitQueryTime = 0.01; // 等待100毫米
    protected $subProcessMaxLoopTimes = 50; // 连续这么多次队列为空就退出子进程
    protected $consumeTryTimes = 3; // 消费3次失败就放弃

    // 子进程没事做则自动退出
    protected $autoQuit = false;

    protected $child_list = [];
    protected $stop_service = false;

    protected $check_internal = 1;

    protected $logFile =  './redis_consumer.log';
    const QUERY_NAME = 'query_name';
    /**
     * @var Redis
     */
    protected $redis = null;

    public function start()
    {

        // 父进程异常，需要终止子进程
        set_exception_handler([$this, 'exceptionHandler']);

        // fork minProcessNum 个子进程
        $this->mutiForkChild($this->minProcessNum);

        if (($processLength = $this->getProcessLength()) <= 0) {
            die('fork 子进程全部失败');
        }

        echo '当前进程数：', $processLength, "\n";

        // 父进程监听信号
        pcntl_signal(SIGTERM, [$this, 'sig_handler']);
        pcntl_signal(SIGINT, [$this, 'sig_handler']);
        pcntl_signal(SIGQUIT, [$this, 'sig_handler']);
        pcntl_signal(SIGCHLD, [$this, 'sig_handler']);

        // 监听队列，队列比进程数多很多，则扩大进程，扩大部分的进程会空闲自动退出

        $this->checkProcessQueueLength();

    }

    protected function log($msg)
    {
        try {
            $this->writeLog($msg, $this->logFile, 'DEBUG');
        } catch (\Exception $e) {
            
        }
    }

    protected function mutiForkChild($num, $autoQuit = false, $maxTryTimes = 3)
    {
        for ($i = 1; $i <= $num; ++$i) {
            $this->forkChild($autoQuit, $maxTryTimes);
        }
    }

    protected function checkProcessQueueLength()
    {
        while (! $this->stop_service) {

            echo '监听队列', "\n";
            $this->log('监听中..');

            // 如果进程数小于最低进程数
            echo '进程差额:', $this->minProcessNum - $this->getProcessLength(), "\n";
            $this->mutiForkChild($this->minProcessNum - $this->getProcessLength());
            
            // 处理进程
            $queueLength = $this->getQueueLength();
            $processLength = $this->getProcessLength();
            $processLength = $processLength <= 0 ? 1 : $processLength; // 避免出现0的情况

            

            
            $this->msleep($this->check_internal);
        }

        echo '退出监听队列', "\n";

        $this->checkExit();
    }

    protected function getProcessLength()
    {
        return count($this->child_list);
    }

    //信号处理函数
    public function sig_handler($sig)
    {

        echo "事件触发：", $sig, "\n";
        switch ($sig) {
            case SIGTERM:
            case SIGINT:
            case SIGQUIT:
                // 退出： 给子进程发送退出信号，退出完成后自己退出

                // 先标记一下,子进程完全退出后才能结束
                $this->stop_service = true;

                // 给子进程发送信号
                foreach ($this->child_list as $pid => $v) {
                    posix_kill($pid, SIGTERM);
                }

                var_dump($this->child_list);
                break;
            case SIGCHLD:
                // 子进程退出, 回收子进程, 并且判断程序是否需要退出
                while (($pid = pcntl_waitpid(-1, $status, WNOHANG)) > 0) {
                    // 去除子进程
                    unset($this->child_list[$pid]);

                    // 子进程是否正常退出
                    if (pcntl_wifexited($status)) {
                        //
                    }
                }

                $this->checkExit();

                break;
        }

    }

    protected function checkExit()
    {
        if ($this->stop_service && empty($this->child_list)) {
            die('父进程结束');
        }
    }

    public function child_sig_handler($sig)
    {
        $this->stop_service = true;
    }

    protected function forkChild($autoQuit = false, $maxTryTimes = 3)
    {

        $times = 1;

        do {

            $pid = pcntl_fork();

            if ($pid == -1) {
                ++$times;
            } elseif($pid) {
                $this->child_list[$pid] = true;
                echo 'pid:', $pid, "\n";
                return $pid;
            } else {
                // 子进程 消费
                $this->autoQuit = $autoQuit;
                // 处理信号
                pcntl_signal(SIGTERM, [$this, 'child_sig_handler']);
                pcntl_signal(SIGINT, [$this, 'child_sig_handler']);
                pcntl_signal(SIGQUIT, [$this, 'child_sig_handler']);
                $this->runChild();
                exit(0); // 子进程结束
            }
        } while ($times <= $maxTryTimes);

        // fork 3次都失败

        return false;

    }

    /**
     * 得到队列长度
     */
    protected function getQueueLength()
    {
        return $this->getRedis()->lLen(static::QUERY_NAME);
    }

    /**
     * 子进程处理内容
     */
    protected function runChild()
    {
        $noDataLoopTime = 0;
        while (!$this->autoQuit || ($noDataLoopTime <= $this->subProcessMaxLoopTimes)) {

            // 处理退出
            if ($this->stop_service) {
                $this->log('进程退出：' . posix_getpid());
                break;
            }

            try {
                if ($data = $this->deQueue()) {
                    $noDataLoopTime = 1; // 从新变从1开始
                    $this->consumeByRetry($data);
                } else {
                    // 避免溢出
                    $noDataLoopTime = $noDataLoopTime >= PHP_INT_MAX ? PHP_INT_MAX : ($noDataLoopTime + 1);
                    // 等待队列
                    $this->msleep($this->waitQueryTime);
                }
            } catch (\RedisException $e) {
                $this->getRedis(true);
                $this->log(['query' => $data, 'status' => $e->getCode(), 'errorMsg' => 'RedisException: ' . $e->getMessage()]);
            } catch (\Exception $e) {
                // 消费出现错误

                $this->log(['query' => $data, 'status' => $e->getCode(), 'errorMsg' => $e->getMessage()]);
            }
        }

        // 处理结束，把redis关闭
        $this->closeRedis();
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
                }
            } catch (\Exception $e) {
                $msg = $e->getMessage();
                ++$tryTimes;
            }
        }
        // 最后一次还报错 写日志
        if ($tryTimes > $this->consumeTryTimes) {
            echo $msg;
        }
    }

    /**
     * 出队
     * @return mixed
     */
    public function deQueue()
    {
        if ($data = $this->getRedis()->lPop(static::QUERY_NAME)) {
            var_dump(['data' => $data]);
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
    protected function consume($data)
    {
        $this->log(['consume' => $data]);
    }

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
    }

    protected function msleep($time)
    {
        usleep($time * 1000000);
    }

    public function exceptionHandler($exception)
    {
        $this->log('父进程['.posix_getpid().']错误退出中:' . $exception->getMessage());
        $this->sig_handler(SIGQUIT);
    }
}

// set_error_handler
// set_exception_handler
// register_shutdown_function