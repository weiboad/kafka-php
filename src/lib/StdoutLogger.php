<?php
/**
 * Created by PhpStorm.
 * User: yangzhen
 * Date: 2018/9/19
 * Time: 18:39
 */

namespace Kafka\lib;


use Psr\Log\AbstractLogger;

class StdoutLogger extends AbstractLogger
{

    /**
     * Logs with an arbitrary level.
     *
     * @param mixed  $level
     * @param string $message
     * @param array  $context
     *
     * @return void
     */
    public function log($level, $message, array $context = array())
    {
        $context_msg = $context ? json_encode($context, JSON_UNESCAPED_UNICODE):'';
        echo date('[Y-m-d H:i:s]'), "[$level]$message", $context_msg, PHP_EOL;
    }
}