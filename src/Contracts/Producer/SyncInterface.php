<?php
namespace Kafka\Contracts\Producer;

interface SyncInterface
{
    public function send($data);
}
