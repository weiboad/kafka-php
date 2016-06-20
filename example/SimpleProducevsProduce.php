<?php
/**************************************************************
  WITH connection pool (SWOOLE[http://www.swoole.com] )
  CLASS Produce TPS: 400message/s
  CLASS SIMPLEPRODUCE TPS: 4000message/s
**************************************************************/
require 'autoloader.php';
$index = 0;
$produce = \Kafka\Produce::getInstance('127.0.0.1:2181', 3000);
$produce->setRequireAck(0);
while(true) {
    if ($index == 0) {
        $start_time = microtime (true);
    }
    $produce->setMessages('test', 0, array('test11111110099090'));
    $result = $produce->send();
    $index++;
    if ($index%2000==0) {
        $end_time = microtime (true);
        echo $index. '|'. ($end_time-$start_time);           /////5s
        $start_time = $end_time;
        break;
    }
}
echo PHP_EOL;
$index = 0;
$produce = \Kafka\SimpleProduce::getInstance('127.0.0.1:2181', 3000);
$produce->setTopic('test', 0);
while(true) {
    if ($index == 0) {
        $start_time = microtime (true);
    }
    $result = $produce->send('test11111110099090');
    $index++;
    if ($index%2000==0) {
        $end_time = microtime (true);
        echo $index. '|'. ($end_time-$start_time);         ///////0.5s
        $start_time = $end_time;
        break;
    }
}
echo PHP_EOL;