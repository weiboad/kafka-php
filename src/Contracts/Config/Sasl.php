<?php
namespace Kafka\Contracts\Config;

interface Sasl
{
	public function getKeytab() : string;

	public function getPrincipal() : string;

	public function getUsername() : string;

	public function getPassword() : string;
}
