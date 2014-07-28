<?php
require 'autoloader.php';

while (1) {
    $part = mt_rand(0, 9);
    $produce = \Kafka\Produce::getInstance('192.168.1.115', '2181');

    $produce->setRequireAck(-1);
    $produce->setMessages('test_conn2', $part, array('test11111110099090'));
    $result = $produce->send();
  //  var_dump($result);
    usleep(10000);
}
