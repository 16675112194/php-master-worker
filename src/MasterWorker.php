<?php
declare(ticks = 1);
// 必须先使用语句declare(ticks=1)，否则注册的singal-handel就不会执行了
//error_reporting(E_ERROR);


abstract class MasterWorker
{

    // 子进程配置属性
    protected $maxWorkerNum; // 最多只能开启进程数
    protected $minWorkerNum; // 最少常驻子进程数
    protected $waitTaskTime; // 等待任务时间，单位秒
    protected $waitTaskLoopTimes; // 连续这么多次队列为空就退出子进程
    protected $consumeTryTimes; // 连续消费失败次数

    // 父进程专用属性
    protected $child_list = [];
    protected $check_internal = 1;

    // 子进程专用属性
    protected $autoQuit = false;

    // 通用属性
    protected $stop_service = false;
    protected $master = false;

    // 通用配置
    protected $logFile;


    public function __construct($options = [])
    {
        $this->initConfig($options);
    }

    protected function initConfig($options = [])
    {
        $defaultConfig = [
            'maxWorkerNum' => 10,
            'minWorkerNum' => 3,
            'waitTaskTime' => 0.01,
            'waitTaskLoopTimes' => 50,
            'consumeTryTimes' => 3,
            'logFile' => './producter_consumer.log',
        ];

        foreach ($defaultConfig as $key => $value) {
            $this->$key = $this->arrayGet($options, $key, $value);
        }
    }

    public function start()
    {

        // 父进程异常，需要终止子进程
        set_exception_handler([$this, 'exceptionHandler']);

        // fork minWorkerNum 个子进程
        $this->mutiForkChild($this->minWorkerNum);

        if (($processLength = $this->getProcessLength()) <= 0) {
            die('fork 子进程全部失败');
        }

        $this->master = true;

        //echo '当前进程数：', $processLength, "\n";

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
            $this->writeLog($msg, $this->logFile, $this->isMaster() ? 'Master' : 'Worker');
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

            $this->msleep($this->check_internal);

            //echo '监听队列', "\n";
            $this->log('监听中..');

            // 处理进程
            $processLength = $this->getProcessLength();

            // 如果进程数小于最低进程数
            //echo '进程差额:', $this->minWorkerNum - $processLength, "\n";
            $this->mutiForkChild($this->minWorkerNum - $processLength);

            $processLength = $this->getProcessLength();

            if ($processLength <= 0) {
                die('创建子进程失败');
            }
            
            if ($processLength >= $this->maxWorkerNum) {
                // 不需要增加进程
                continue;
            }

            // 简单的算法来增加
            $queueLength = $this->getQueueLength();

            // 还不够多
            if (($queueLength / $processLength < 3) && ($queueLength - $processLength < 10)) {
                continue;
            }

            // 增加一定数量的进程
            $num = ceil(($this->maxWorkerNum - $this->processLength ) / 2);

            // 新建进程，空闲自动退出
            $this->mutiForkChild($num, true);
            //echo '新增进程：', $num, "\n";
        }

        //echo '退出监听队列', "\n";

        $this->checkExit();
    }

    protected function getProcessLength()
    {
        return count($this->child_list);
    }

    //信号处理函数
    public function sig_handler($sig)
    {

        $this->log("接受信号处理：" . $sig);
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

    public function child_sig_handler($sig)
    {
        $this->stop_service = true;
    }

    protected function checkExit()
    {
        if ($this->stop_service && empty($this->child_list)) {
            die('父进程结束');
        }
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
                //echo 'pid:', $pid, "\n";
                return $pid;
            } else {
                // 子进程 消费
                $this->autoQuit = $autoQuit;
                // 处理信号
                pcntl_signal(SIGTERM, [$this, 'child_sig_handler']);
                pcntl_signal(SIGINT, [$this, 'child_sig_handler']);
                pcntl_signal(SIGQUIT, [$this, 'child_sig_handler']);
                exit($this->runChild()); // 子进程结束
            }
        } while ($times <= $maxTryTimes);

        // fork 3次都失败

        return false;

    }

    /**
     * 得到队列长度
     */
    abstract protected function getQueueLength();

    /**
     * 子进程处理内容
     */
    protected function runChild()
    {
        $noDataLoopTime = 0;
        $status = 0;
        while (!$this->autoQuit || ($noDataLoopTime <= $this->waitTaskLoopTimes)) {

            // 处理退出
            if ($this->stop_service) {
                break;
            }

            try {
                $data = $this->deQueue();
                if ($data) {
                    $noDataLoopTime = 1; // 从新变从1开始
                    $this->consumeByRetry($data);
                } else {
                    // 避免溢出
                    $noDataLoopTime = $noDataLoopTime >= PHP_INT_MAX ? PHP_INT_MAX : ($noDataLoopTime + 1);
                    // 等待队列
                    $this->msleep($this->waitTaskTime);
                }

                $status = 0;
            } catch (\RedisException $e) {
                $this->log(['data' => $data, 'status' => $e->getCode(), 'errorMsg' => 'RedisException: ' . $e->getMessage()]);
                $status = 1;
            } catch (\Exception $e) {
                // 消费出现错误
                $this->log(['data' => $data, 'status' => $e->getCode(), 'errorMsg' => $e->getMessage()]);
                $status = 2;
            }
        }

        $this->childbeforeExit();

        return $status;
    }

    /**
     * @param $data
     * @param int $tryTimes
     * @throws \Exception
     */
    protected function consumeByRetry($data, $tryTimes = 1)
    {
        $tryTimes = 1;
        $exception = null;
        // consume 返回false 为失败
        while ($tryTimes <= $this->consumeTryTimes) {
            try {
                if ($this->consume($data)) {
                    break; // 执行成功
                } else {
                    ++$tryTimes;
                }
            } catch (\Exception $e) {
                $exception = $e;
                ++$tryTimes;
            }
        }
        // 最后一次还报错 写日志
        if (($tryTimes > $this->consumeTryTimes) && $exception) {
            throw $exception;
        }
    }

    /**
     * 出队
     * @return mixed
     */
    abstract public function deQueue();

    /**
     * 入队
     * @param $data
     * @return int
     */
    abstract public function enQueue($data);

    /**
     * 消费的具体内容
     * 不要进行失败重试
     * 会自动进行
     * 如果失败最好直接抛出异常
     * @param $data
     */
    abstract protected function consume($data);

    /**
     * 子进程结束回调
     *
     * @return void
     */
    abstract protected function childbeforeExit();

    /**
     * @param $mixed
     * @param $filename
     * @param $header
     * @param bool $trace
     * @return bool
     * @throws \Exception
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
            throw new \Exception('Could not open logfile:' . $filename);
        }
        // exclusive lock, will get released when the file is closed
        if (! flock($h, LOCK_EX)) {
            return false;
        }
        if (fwrite($h, $text) === false) {
            throw new \Exception('Could not write to logfile:' . $filename);
        }
        flock($h, LOCK_UN);
        fclose($h);
        return true;
    }

    protected function closeRedis()
    {
        $this->redis && $this->redis->close();
        $this->redis = null;
        $this->log('redis 关闭');
    }

    protected function msleep($time)
    {
        usleep($time * 1000000);
    }

    public function exceptionHandler($exception)
    {
        if ($this->isMaster()) {
            $this->log('父进程['.posix_getpid().']错误退出中:' . $exception->getMessage());
            $this->sig_handler(SIGQUIT);
        } else {
            $this->child_sig_handler(SIGQUIT);
        }
    }

    public function isMaster()
    {
        return $this->master;
    }

    protected function arrayGet($array, $key, $default = null)
    {
        return array_key_exists($key, $array) ? $array[$key] : $default;
    }
}

// set_error_handler
// set_exception_handler
// register_shutdown_function