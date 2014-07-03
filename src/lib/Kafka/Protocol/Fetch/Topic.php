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

class Topic implements \Iterator, \Countable
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
     * validCount 
     * 
     * @var float
     * @access private
     */
    private $validCount = 0;

    /**
     * topic count 
     * 
     * @var float
     * @access private
     */
    private $topicCount = false;

    /**
     * current topic 
     * 
     * @var mixed
     * @access private
     */
    private $current = null;

    /**
     * current iterator key 
     * topic name
     * 
     * @var string
     * @access private
     */
    private $key = null;

    /**
     * valid 
     * 
     * @var mixed
     * @access private
     */
    private $valid = false;

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
    public function __construct(\Kafka\Socket $stream)
    {
        $this->stream = $stream;
        $this->topicCount = $this->getTopicCount();
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
        return $this->key; 
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
        $this->valid = $this->loadNextTopic();
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
        return $this->valid && ($this->validCount <= $this->topicCount);
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
        $this->valid = $this->loadNextTopic();
    }

    // }}}
    // {{{ public function count()

    /**
     * implements Countable function 
     * 
     * @access public
     * @return integer
     */
    public function count()
    {
        return $this->topicCount;
    }

    // }}}
    // {{{ protected function getTopicCount()

    /**
     * get message size 
     * only use to object init
     * 
     * @access protected
     * @return integer
     */
    protected function getTopicCount()
    {
        // read topic count
        $data = $this->stream->read(4, true);
        $data = unpack('N', $data);
        $count = array_shift($data); 
        if ($count <= 0) {
            throw new \Kafka\Exception\OutOfRange($size . ' is not a valid topic count');
        }

        return $count;
    }

    // }}}
    // {{{ public function loadNextTopic()
    
    /**
     * load next topic 
     * 
     * @access public
     * @return void
     */
    public function loadNextTopic()
    {
        if ($this->validCount >= $this->topicCount) {
            return false;
        } 
        
        try {
            $topicLen = $this->stream->read(2, true);
            $topicLen = unpack('n', $topicLen);
            $topicLen = array_shift($topicLen);
            if ($topicLen <= 0) {
                return false;    
            }

            // topic name
            $this->key = $this->stream->read($topicLen, true);
            $this->current = new Partition($this->stream); 
        } catch (\Kafka\Exception $e) {
            return false;
        }

        $this->validCount++;

        return true;
    }

    // }}}
    // }}}
}
