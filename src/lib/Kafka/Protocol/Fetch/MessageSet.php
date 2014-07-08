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

namespace Kafka\Protocol\Fetch;

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
     * messageSet size 
     * 
     * @var float
     * @access private
     */
    private $messageSetSize = 0;

    /**
     * validByteCount 
     * 
     * @var float
     * @access private
     */
    private $validByteCount = 0;

    /**
     * messageSet offset 
     * 
     * @var float
     * @access private
     */
    private $offset = 0;

    /**
     * valid 
     * 
     * @var mixed
     * @access private
     */
    private $valid = false;

    /**
     * partition object  
     * 
     * @var \Kafka\Protocol\Fetch\Partition
     * @access private
     */
    private $partition = null;

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
    public function __construct(\Kafka\Protocol\Fetch\Partition $partition)
    {
        $this->stream = $partition->getStream();
        $this->partition = $partition;
        $this->messageSetSize = $this->getMessageSetSize();
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
        return $this->current; 
    }

    // }}}
    // {{{ public function key()

    /**
     * key 
     * 
     * @access public
     * @return void
     */
    public function key()
    {
        return $this->validByteCount; 
    }

    // }}}
    // {{{ public function rewind()

    /**
     * implements Iterator function 
     * 
     * @access public
     * @return integer
     */
    public function rewind()
    {
        $this->valid = $this->loadNextMessage();
    }

    // }}}
    // {{{ public function valid()

    /**
     * implements Iterator function 
     * 
     * @access public
     * @return integer
     */
    public function valid()
    {
        if (!$this->valid) {
            // one partition iterator end 
            \Kafka\Protocol\Fetch\Helper\Helper::onPartitionEof($this->partition);
        }

        return $this->valid;
    }

    // }}}
    // {{{ public function next()

    /**
     * implements Iterator function 
     * 
     * @access public
     * @return integer
     */
    public function next()
    {
        $this->valid = $this->loadNextMessage();
    }

    // }}}
    // {{{ protected function getMessageSetSize()

    /**
     * get message set size 
     * 
     * @access protected
     * @return integer
     */
    protected function getMessageSetSize()
    {
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
    // {{{ public function loadNextMessage()
    
    /**
     * load next message 
     * 
     * @access public
     * @return void
     */
    public function loadNextMessage()
    {
        if ($this->validByteCount >= $this->messageSetSize) {
            return false;
        } 
        
        try {
            $offset = $this->stream->read(8, true);
            $this->offset  = \Kafka\Protocol\Decoder::unpackInt64($offset);
            $messageSize = $this->stream->read(4, true);
            $messageSize = unpack('N', $messageSize);
            $messageSize = array_shift($messageSize);
            $msg  = $this->stream->read($messageSize, true);

            $this->current = new Message($msg); 
        } catch (\Kafka\Exception $e) {
            return false;
        }

        $this->validByteCount += 8 + 4 + $messageSize;

        return true;
    }

    // }}}
    // }}}
}
