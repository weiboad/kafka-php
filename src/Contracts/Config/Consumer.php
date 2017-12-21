<?php
namespace Kafka\Contracts\Config;

interface Consumer
{
    public function getGroupId() : string;
    public function getSessionTimeout() : int;
    public function getRebalanceTimeout() : int;
    public function getTopics() : array;
    public function getOffsetReset() : string;
    public function getMaxBytes() : int;
    public function getMaxWaitTime() : int;
    public function getConsumeMode() : int;
}
