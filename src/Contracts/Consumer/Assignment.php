<?php
namespace Kafka\Contracts\Consumer;

use DI\FactoryInterface;

interface Assignment
{
    public function assign(FactoryInterface $container, array $members): void;

	public function getAssignments() : array;
}
