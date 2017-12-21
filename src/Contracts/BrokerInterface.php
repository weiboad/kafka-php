<?php
namespace Kafka\Contracts;

interface BrokerInterface
{
    public function setProcess(callable $process) : void;
    public function setGroupBrokerId(int $brokerId) : void;
    public function getGroupBrokerId() : int;
    public function setData(array $topics, array $brokersResult) : bool;
    public function getTopics() : array;
    public function getBrokers() : array;
    public function getRandConnect() : SocketInterface;
    public function getMetaConnect(string $key) : SocketInterface;
    public function getDataConnect(string $key) : SocketInterface;
    public function clear() : void;
}
