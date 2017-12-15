<?php
namespace Kafka\Config;

use Kafka\Exception;
use Kafka\Contracts\Config\Broker as BrokerInterface;

class Broker implements BrokerInterface
{
	private $clientId = 'kafka-php';

	private $version = '0.10.2.0';

	private $metadataBrokerList = '';

	private $metadataRequestTimeoutMs = 60000;

	private $metadataRefreshIntervalMs = 300000;

	private $messageMaxBytes = 1000000;

	private $metadataMaxAgeMs = -1;

	public function getClientId() : string 
	{
		return $this->clientId;	
	}

	public function getVersion() : string
	{
		return $this->version;
	}

	public function getMetadataBrokerList() : string
	{
		if ('' === $this->metadataBrokerList) {
			throw new Exception('Invalid metadata broker list, must give valid broker list.');
		}
		return $this->metadataBrokerList;	
	}

	public function getMetadataRequestTimeoutMs() : int
	{
		return $this->metadataRequestTimeoutMs;	
	}

	public function getMetadataRefreshIntervalMs() : int
	{
		return $this->metadataRefreshIntervalMs;
	}

	public function getMetadataMaxAgeMs() : int
	{
		return $this->metadataMaxAgeMs; 
	}

	public function getMessageMaxBytes() : int
	{
		return $this->messageMaxBytes;
	}

    public function setClientId(string $val) : void
    {
        $client = trim($val);
        if ($client == '') {
            throw new \Kafka\Exception\Config('Set clientId value is invalid, must is not empty string.');
        }
		$this->clientId = $client;
    }

    public function setVersion(string $version) : void
    {
        $version = trim($version);
        if ($version == '' || version_compare($version, '0.8.0') < 0) {
            throw new \Kafka\Exception\Config('Set broker version value is invalid, must is not empty string and gt 0.8.0.');
        }
		$this->version = $version;
    }

    public function setMetadataBrokerList(string $list) : void
    {
        if (trim($list) == '') {
            throw new \Kafka\Exception\Config('Set broker list value is invalid, must is not empty string');
        }
        $tmp   = explode(',', trim($list));
        $lists = [];
        foreach ($tmp as $key => $val) {
            if (trim($val) != '') {
                $lists[] = $val;
            }
        }
        if (empty($lists)) {
            throw new \Kafka\Exception\Config('Set broker list value is invalid, must is not empty string');
        }
        foreach ($lists as $val) {
            $hostinfo = explode(':', $val);
            foreach ($hostinfo as $key => $val) {
                if (trim($val) == '') {
                    unset($hostinfo[$key]);
                }
            }
            if (count($hostinfo) != 2) {
                throw new \Kafka\Exception\Config('Set broker list value is invalid, must is not empty string');
            }
        }
		$this->metadataBrokerList = $list;
    }

    public function setMessageMaxBytes(int $messageMaxBytes) : void
    {
        if (! is_numeric($messageMaxBytes) || $messageMaxBytes < 1000 || $messageMaxBytes > 1000000000) {
            throw new \Kafka\Exception\Config('Set message max bytes value is invalid, must set it 1000 .. 1000000000');
        }
		$this->messageMaxBytes = $messageMaxBytes;
    }

    public function setMetadataRequestTimeoutMs(int $metadataRequestTimeoutMs) : void
    {
        if (! is_numeric($metadataRequestTimeoutMs) || $metadataRequestTimeoutMs < 10
            || $metadataRequestTimeoutMs > 900000) {
            throw new \Kafka\Exception\Config('Set metadata request timeout value is invalid, must set it 10 .. 900000');
        }
		$this->metadataRequestTimeoutMs = $metadataRequestTimeoutMs;
    }

    public function setMetadataRefreshIntervalMs(int $metadataRefreshIntervalMs) : void
    {
        if (! is_numeric($metadataRefreshIntervalMs) || $metadataRefreshIntervalMs < 10
            || $metadataRefreshIntervalMs > 3600000) {
            throw new \Kafka\Exception\Config('Set metadata refresh interval value is invalid, must set it 10 .. 3600000');
        }
		$this->metadataRefreshIntervalMs = $metadataRefreshIntervalMs;
    }

    public function setMetadataMaxAgeMs(int $metadataMaxAgeMs) : void
    {
        if (! is_numeric($metadataMaxAgeMs) || $metadataMaxAgeMs < 1
            || $metadataMaxAgeMs > 86400000) {
            throw new \Kafka\Exception\Config('Set metadata max age value is invalid, must set it 1 .. 86400000');
        }
		$this->metadataMaxAgeMs = $metadataMaxAgeMs;
    }
}
