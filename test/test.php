<?php

require "./RedisProductConsumer.php";

$consumer = new RedisProductConsumer();
$consumer->start();