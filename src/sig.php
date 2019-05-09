<?php
//使用ticks需要PHP 4.3.0以上版本
declare(ticks = 1);

//信号处理函数
function sig_handler($signo)
{

     switch ($signo) {
         case SIGTERM:
             // 处理SIGTERM信号
             echo "SIGTERM 触发\n";
             exit;
             break;
        case SIGINT:
             // 处理SIGINT信号
             echo "SIGINT 触发\n";
             exit;
             break;
         case SIGHUP:
             //处理SIGHUP信号
             echo "SIGHUP\n";
             break;
         case SIGUSR1:
             echo "Caught SIGUSR1...\n";
             break;
         default:
             // 处理所有其他信号
     }

}

echo "Installing signal handler...\n";

//安装信号处理器
pcntl_signal(SIGTERM, "sig_handler");
pcntl_signal(SIGHUP,  "sig_handler");
pcntl_signal(SIGUSR1, "sig_handler");
pcntl_signal(SIGINT, "sig_handler");
pcntl_signal(SIGQUIT, "sig_handler");

// 或者在PHP 4.3.0以上版本可以使用对象方法
// pcntl_signal(SIGUSR1, array($obj, "do_something");

echo "Generating signal SIGTERM to self...\n";

//向当前进程发送SIGUSR1信号
// posix_kill(posix_getpid(), SIGTERM);

sleep(50);

echo "Done\n";

