<?php
namespace Kafka\Consumer;

class Data
{
    private $memberId = '';

    private $generationId = '';

    private $topics = [];

    private $offsets = [];

    private $lastOffsets = [];

    private $fetchOffsets = [];

    private $consumerOffsets = [];

    private $commitOffsets = [];

    private $precommitOffsets = [];

    public function setMemberId($memberId)
    {
        $this->memberId = $memberId;
    }

    public function getMemberId()
    {
        return $this->memberId;
    }

    public function setGenerationId($generationId)
    {
        $this->generationId = $generationId;
    }

    public function getGenerationId()
    {
        return $this->generationId;
    }

    public function setTopics($topics)
    {
        $this->topics = $topics;
    }

    public function getTopics()
    {
        return $this->topics;
    }

    public function setOffsets($offsets)
    {
        $this->offsets = $offsets;
    }

    public function getOffsets()
    {
        return $this->offsets;
    }

    public function setLastOffsets($offsets)
    {
        $this->lastOffsets = $offsets;
    }

    public function getLastOffsets()
    {
        return $this->lastOffsets;
    }

    public function setFetchOffsets($offsets)
    {
        $this->fetchOffsets = $offsets;
    }

    public function getFetchOffsets()
    {
        return $this->fetchOffsets;
    }

    public function setConsumerOffsets($offsets)
    {
        $this->consumerOffsets = $offsets;
    }

    public function getConsumerOffsets()
    {
        return $this->consumerOffsets;
    }

    public function setConsumerOffset($topic, $part, $offset)
    {
        $this->consumerOffsets[$topic][$part] = $offset;
    }

    public function getConsumerOffset($topic, $part)
    {
        if (! isset($this->consumerOffsets[$topic][$part])) {
            return false;
        }
        return $this->consumerOffsets[$topic][$part];
    }

    public function setCommitOffsets($offsets)
    {
        $this->commitOffsets = $offsets;
    }

    public function getCommitOffsets()
    {
        return $this->commitOffsets;
    }

    public function setCommitOffset($topic, $part, $offset)
    {
        $this->commitOffsets[$topic][$part] = $offset;
    }

    public function setPrecommitOffsets($offsets)
    {
        $this->precommitOffsets = $offsets;
    }

    public function getPrecommitOffsets()
    {
        return $this->precommitOffsets;
    }

    public function setPrecommitOffset($topic, $part, $offset)
    {
        $this->precommitOffsets[$topic][$part] = $offset;
    }

    public function clearOffset()
    {
        $this->offsets          = [];
        $this->lastOffsets      = [];
        $this->fetchOffsets     = [];
        $this->consumerOffsets  = [];
        $this->commitOffsets    = [];
        $this->precommitOffsets = [];
    }
}
