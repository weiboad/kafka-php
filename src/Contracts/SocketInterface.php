<?php
namespace Kafka\Contracts;

interface SocketInterface
{
    public function connect();
    public function read($data);
    public function write($data);
    public function readBlocking(int $len) : string;
    public function writeBlocking(string $data) : int;
    public function setOnReadable(callable $process) : void;
    public function getSocket();
}
