<?php

require "./RedisProducterConsumer.php";

$consumer = new RedisProducterConsumer();
$consumer->start();