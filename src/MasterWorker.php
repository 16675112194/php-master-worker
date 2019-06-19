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
    protected $worker_list = [];
    protected $check_internal = 1;
    protected $masterExitCallback = [];

    // 子进程专用属性
    protected $autoQuit = false;
    protected $status = self::WORKER_STATUS_IDLE;
    protected $taskData; // 任务数据
    protected $workerExitCallback = [];

    // 通用属性
    protected $stop_service = false;
    protected $master_pid = 0; // Master 进程的PID
    protected $process_pid = 0; // 本进程 PID

    // 通用配置
    protected $logFile;

    const WORKER_STATUS_IDLE = 'idle';
    const WORKER_STATUS_FINISHED = 'finished';
    const WORKER_STATUS_EXITING = 'exiting';
    const WORKER_STATUS_WORKING = 'working';
    const WORKER_STATUS_FAIL = 'fail';
    const WORKER_STATUS_TERMINATED = 'terminated';


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
            'logFile' => './master_worker.log',
        ];

        foreach ($defaultConfig as $key => $default) {
            $this->$key = array_key_exists($key, $options) ? $options[$key] : $default;
        }
    }

    public function start()
    {

        // 父进程异常，需要终止子进程
        set_exception_handler([$this, 'exceptionHandler']);

        $this->master_pid = $this->process_pid = getmypid();

        // fork minWorkerNum 个 常驻 Worker 并且延时运行，等到父进程设置好信号回调
        $this->mutiForkWorker($this->minWorkerNum, false, 0.1);

        if ($this->getWorkerLength() <= 0) {
            $this->masterWaitExit(true, 'fork 子进程全部失败');
        }

        // 父进程监听信号
        pcntl_signal(SIGTERM, [$this, 'sig_handler']);
        pcntl_signal(SIGINT, [$this, 'sig_handler']);
        pcntl_signal(SIGQUIT, [$this, 'sig_handler']);
        pcntl_signal(SIGCHLD, [$this, 'sig_handler']);

        // 监听队列，队列比进程数多很多，则扩大进程，扩大部分的进程会空闲自动退出
        $this->checkWorkerLength();

        $this->masterWaitExit();
    }

    /**
     * Master 等待退出
     *
     * @param boolean $force 强制退出
     * @param string $msg 退出 message
     * @return void
     */
    protected function masterWaitExit($force = false, $msg = '')
    {
        // 强制发送退出信号
        $force && $this->sig_handler(SIGTERM);

        // 等到子进程退出
        while ($this->stop_service) {
            $this->checkExit($msg);
            $this->msleep($this->check_internal);
        }
    }

    protected function log($msg)
    {
        try {
            $header = $this->isMaster() ? 'Master [permanent]' : sprintf('Worker [%s]', $this->autoQuit ? 'temporary' : 'permanent');
            $this->writeLog($msg, $this->getLogFile(), $header);
        } catch (\Exception $e) {
            
        }
    }

    protected function mutiForkWorker($num, $autoQuit = false, $delay = 0, $maxTryTimes = 3)
    {
        for ($i = 1; $i <= $num; ++$i) {
            $this->forkWorker($autoQuit, $maxTryTimes, $delay);
        }
    }

    protected function checkWorkerLength()
    {
        // 如果要退出父进程，就不执行检测
        while (! $this->stop_service) {

            $this->msleep($this->check_internal);

            // 处理进程
            $workerLength = $this->getWorkerLength();

            // 如果进程数小于最低进程数
            $this->mutiForkWorker($this->minWorkerNum - $workerLength);

            $workerLength = $this->getWorkerLength();

            // 创建常驻worker进程失败, 下次检查继续尝试创建
            if ($workerLength <= 0) {
                continue;
            }
            
            if ($workerLength >= $this->maxWorkerNum) {
                // 不需要增加进程
                continue;
            }

            $num = $this->calculateAddWorkerNum();

            // 不允许超过最大进程数
            $num = min($num, $this->maxWorkerNum - $workerLength);

            // 创建空闲自动退出worker进程
            $this->mutiForkWorker($num, true);

        }
    }

    protected function getWorkerLength()
    {
        return count($this->worker_list);
    }

    /**
     * Master 进程信号处理函数
     */
    public function sig_handler($sig)
    {
        switch ($sig) {
            case SIGTERM:
            case SIGINT:
            case SIGQUIT:
                // 退出： 给子进程发送退出信号，退出完成后自己退出

                // 先标记一下,子进程完全退出后才能结束
                $this->stop_service = true;

                // 给子进程发送信号
                foreach ($this->worker_list as $pid => $v) {
                    posix_kill($pid, SIGTERM);
                }

                break;
            case SIGCHLD:
                // 子进程退出, 回收子进程, 并且判断程序是否需要退出
                while (($pid = pcntl_waitpid(-1, $status, WNOHANG)) > 0) {
                    // 去除子进程
                    unset($this->worker_list[$pid]);

                    // 子进程是否正常退出
                    // if (pcntl_wifexited($status)) {
                    //     //
                    // }
                }

                $this->checkExit();

                break;
            default:
                $this->default_sig_handler($sig);
                break;
        }

    }

    /**
     * Worker 进程信号处理函数
     */
    public function child_sig_handler($sig)
    {
        switch ($sig) {
            case SIGINT:
            case SIGQUIT:
            case SIGTERM:
                $this->stop_service = true;
                break;
            // 操作比较危险 在处理任务当初强制终止
            // case SIGTERM:
            //     // 强制退出
            //     $this->stop_service = true;
            //     $this->status = self::WORKER_STATUS_TERMINATED;
            //     $this->beforeWorkerExitHandler();
            //     $this->status = self::WORKER_STATUS_EXITING;
            //     die(1);
            //     break;
        }
    }

    /**
     * Master 进程检查退出
     */
    protected function checkExit($msg = '')
    {
        if ($this->stop_service && empty($this->worker_list)) {
            $this->beforeMasterExitHandler();
            die($msg ?:'Master 进程结束, Worker 进程全部退出');
        }
    }

    /**
     * Fork Worker 进程
     */
    protected function forkWorker($autoQuit = false, $maxTryTimes = 3, $delay = 0)
    {

        $times = 1;

        do {

            $pid = pcntl_fork();

            if ($pid == -1) {
                ++$times;
            } elseif($pid) {
                $this->worker_list[$pid] = true;
                return $pid;
            } else {
                // 延时运行, 初始Fork Worker 需要先等Master 设置好信号处理回调
                // 避免Master未设置回调，Worker就异常退出，无法回收资源
                $this->msleep($delay);
                // 子进程 这里需要重新初始化Worker参数
                $this->autoQuit = $autoQuit;
                $this->process_pid = getmypid();

                // 处理信号
                pcntl_signal(SIGTERM, [$this, 'child_sig_handler']);
                pcntl_signal(SIGINT, [$this, 'child_sig_handler']);
                pcntl_signal(SIGQUIT, [$this, 'child_sig_handler']);

                // 自定义Worker初始化
                $this->initWorker();
                exit($this->workerHandler()); // worker进程结束
            }
        } while ($times <= $maxTryTimes);

        // fork 3次都失败

        return false;

    }

    /**
     * 判断Master 进程是否退出
     */
    protected function masterIsExited()
    {
        if ($this->isMaster()) {
            // Master 检查自己是否退出，说明没有退出
            return false;
        } else {
            // master 进程PID 不是 Worker 的父进程 PID 说明：Worker 进程成为孤儿进程，被 init 进程接管(PID = 0)
            return $this->master_pid !== posix_getppid();
        }
    }

    /**
     * 子进程处理 Handler
     */
    protected function workerHandler()
    {
        $noDataLoopTime = 0;
        $status = 0;
        while (!$this->autoQuit || ($noDataLoopTime <= $this->waitTaskLoopTimes)) {

            // 处理退出
            if ($this->stop_service) {
                break;
            }

            // 如果空闲， 那么常驻进程 需要检查父进程是否已经挂掉
            if (($noDataLoopTime > $this->waitTaskLoopTimes) && $this->masterIsExited()) {
                // 如果 Master 已经挂掉，那么 Worker 也退出
                break;
            }

            $this->taskData = null;
            try {
                $this->taskData = $this->deQueue();
                if ($this->taskData) {
                    $noDataLoopTime = 1; // 重新从1开始
                    $this->status = self::WORKER_STATUS_WORKING;
                    $this->consumeByRetry($this->taskData);
                    $this->status = self::WORKER_STATUS_FINISHED;
                } else {
                    $this->status = self::WORKER_STATUS_IDLE;
                    // 避免溢出 最大值只有 (PHP_INT_MAX - 1)
                    $noDataLoopTime = min(PHP_INT_MAX - 1, $noDataLoopTime + 1);
                    // 等待队列
                    $this->msleep($this->waitTaskTime);
                }

                $status = 0;
            } catch (\RedisException $e) {
                $this->status = self::WORKER_STATUS_FAIL;
                $this->consumeFail($this->taskData, $e);
                $status = 1;
            } catch (\Exception $e) {
                // 消费出现错误
                $this->status = self::WORKER_STATUS_FAIL;
                $this->consumeFail($this->taskData, $e);
                $status = 2;
            }
        }

        $this->beforeWorkerExitHandler();
        $this->status = self::WORKER_STATUS_EXITING;

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
                return $this->consume($data);
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
        $text = "\r\n=" . $header . "==== " . strftime("[%Y-%m-%d %H:%M:%S] ") . " ===\r\n<" . $this->process_pid . "> : " . $text . "\r\n" . $trace_list;
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

    protected function msleep($time)
    {
        usleep($time * 1000000);
    }

    /**
     * 异常处理
     */
    public function exceptionHandler($exception)
    {
        if ($this->isMaster()) {
            $msg = 'Master进程错误退出中:' . $exception->getMessage();
            $this->log($msg);
            $this->masterWaitExit(true, $msg);
        } else {
            $this->log('Worker进程错误退出中:' . $exception->getMessage());
            $this->child_sig_handler(SIGTERM);
        }
    }

    /**
     * 是否 Master 进程
     */
    public function isMaster()
    {
        return $this->master_pid === $this->process_pid;
    }

    /**
     * 默认的 worker 数量增加处理
     * 
     * @return int
     */
    public function calculateAddWorkerNum()
    {
        $workerLength = $this->getWorkerLength();
        $taskLength = $this->getTaskLength();
        // 还不够多
        if (($taskLength / $workerLength < 3) && ($taskLength - $workerLength < 10)) {
            return 0;
        }

        // 增加一定数量的进程
        return ceil($this->maxWorkerNum - $workerLength / 2);
    }

    /**
     * 自定义日子文件
     *
     * @return string
     */
    protected function getLogFile()
    {
        return $this->logFile;
    }

    /**
     * 自定义消费错误函数
     *
     * @param [type] $data
     * @param \Exception $e
     * @return void
     */
    protected function consumeFail($data, \Exception $e)
    {
        $this->log(['data' => $data, 'errorCode' => $e->getCode(), 'errorMsg' => get_class($e) . ' : ' . $e->getMessage()]);
    }

     protected function beforeWorkerExitHandler()
     {
         foreach ($this->workerExitCallback as $callback) {
            is_callable($callback) && call_user_func($callback, $this);
         }
     }

     /**
      * 设置Worker自定义结束回调
      *
      * @param mixed  $func
      * @param boolean $prepend
      * @return void
      */
     public function setWorkerExitCallback($callback, $prepend = false)
     {
        return $this->setCallbackQueue('workerExitCallback', $callback, $prepend);
     }

     /**
     * 设置Master自定义结束回调
     *
     * @param callable $func
     * @param boolean $prepend
     * @return void
     */
    public function setMasterExitCallback(callable $callback, $prepend = false)
    {
        return $this->setCallbackQueue('masterExitCallback', $callback, $prepend);
    }

    protected function setCallbackQueue($queueName, $callback, $prepend = false)
    {
        if (! isset($this->$queueName) || ! is_array($this->$queueName)) {
            return false;
        }

        if (is_null($callback)) {
            $this->$queueName = []; // 如果传递 null 就清空
            return true;
        } elseif (! is_callable($callback)) {
            return false;
        }

        if ($prepend) {
            array_unshift($this->$queueName, $callback);
        } else {
            $this->$queueName[] = $callback;
        }

        return true;
    }

    protected function beforeMasterExitHandler()
    {
        foreach ($this->masterExitCallback as $callback) {
            is_callable($callback) && call_user_func($callback, $this);
         }
    }

    protected function default_sig_handler($sig)
    {

    }

    /**
     * 重新初始化Worker环境参数
     * 避免Master 与 Worker 共享同一个数据, 比如 网络连接-redis等
     *
     * @param bool $autoQuit
     * @return void
     */
    protected function initWorker($autoQuit)
    {

    }

    /**
     * 得到待处理任务数量
     */
    abstract protected function getTaskLength();

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
     * 如果失败直接抛出异常
     * @param $data
     */
    abstract protected function consume($data);
}

// set_error_handler
// set_exception_handler
// register_shutdown_function