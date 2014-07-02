<?php
/* vim: set expandtab tabstop=4 shiftwidth=4 softtabstop=4 foldmethod=marker: */
// +---------------------------------------------------------------------------
// | SWAN [ $_SWANBR_SLOGAN_$ ]
// +---------------------------------------------------------------------------
// | Copyright $_SWANBR_COPYRIGHT_$
// +---------------------------------------------------------------------------
// | Version  $_SWANBR_VERSION_$
// +---------------------------------------------------------------------------
// | Licensed ( $_SWANBR_LICENSED_URL_$ )
// +---------------------------------------------------------------------------
// | $_SWANBR_WEB_DOMAIN_$
// +---------------------------------------------------------------------------

namespace Kafka\Protocol;

/**
+------------------------------------------------------------------------------
* Kafka protocol since Kafka v0.8 
+------------------------------------------------------------------------------
* 
* @package 
* @version $_SWANBR_VERSION_$
* @copyright Copyleft
* @author $_SWANBR_AUTHOR_$ 
+------------------------------------------------------------------------------
*/

class MessageSet implements \Iterator
{
    // {{{ members

    /**
     * kafka socket object 
     * 
     * @var mixed
     * @access private
     */
    private $stream = null;

    /**
     * init read bytes 
     * 
     * @var float
     * @access private
     */
    private $initOffset = 0;

    /**
     * validByteCount 
     * 
     * @var float
     * @access private
     */
    private $validByteCount = 0;

    // }}}
    // {{{ functions
    // {{{ public function __construct()

    /**
     * __construct 
     * 
     * @param \Kafka\Socket $stream 
     * @param int $initOffset 
     * @access public
     * @return void
     */
    public function __construct(\Kafka\Socket $stream, $initOffset = 0)
    {
        $this->stream = $stream;
        $this->initOffset = $initOffset; 
    }

    // }}}
    // {{{ public function current()

    /**
     * current 
     * 
     * @access public
     * @return void
     */
    public function current()
    {
        
    }

    // }}}
    // {{{ protected function getMessageSize()

    /**
     * get message size 
     * 
     * @access protected
     * @return integer
     */
    protected function getMessageSize()
    {
        // read offset
        $this->stream->read(8, true);

        // read message size
        $data = $this->stream->read(4, true);
        $data = unpack('N', $data);
        $size = array_shift($data); 
        if ($size <= 0) {
            throw new \Kafka\Exception\OutOfRange($size . ' is not a valid message size');
        }

        return $size;
    }

    // }}}
    // {{{ protected function getMessage()

    /**
     * get message data 
     * 
     * @access protected
     * @return string (raw)
     */
    protected function getMessage()
    {
        try {
            $size = $this->getMessageSize();
            $msg  = $this->stream->read($size, true);
        } catch (\Kafka\Exception\SocketEOF $e) {
            $size = isset($size) ? $size : 'enough';
            $logMsg = 'Cannot read ' . $size . ' bytes, the message is likely bigger than the buffer - original exception: ' . $e->g  etMessage();
            throw new \Kafka\Exception\OutOfRange($logMsg);
        }

        // offset int64 +  messageSize size int32 + message size
        $this->validByteCount += 4 + 8 + $size;
        return $msg; 
    }

    // }}}
    // {{{ public function validBytes()

    /**
     * Get message set size in bytes
     * 
     * @return integer
     */
    public function validBytes()
    {
        return $this->validByteCount;
    }

    // }}}
    // {{{ public function sizeInBytes()

    /**
     * Get message set size in bytes
     * 
     * @return integer
     */
    public function sizeInBytes()
    {
        return $this->validBytes();
    }

    // }}}
    // }}}
}
