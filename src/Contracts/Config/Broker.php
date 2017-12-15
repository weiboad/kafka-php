<?php
namespace Kafka\Contracts\Config;

interface Broker 
{
	public function getClientId() : string;

	public function getVersion() : string;

	public function getMetadataBrokerList() : string;

	public function getMetadataRequestTimeoutMs() : int;

	public function getMetadataRefreshIntervalMs() : int;

	public function getMetadataMaxAgeMs() : int;

	public function getMessageMaxBytes() : int;
}
