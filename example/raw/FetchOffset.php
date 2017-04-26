<?php
require '../../vendor/autoload.php';
date_default_timezone_set('PRC');
use Monolog\Logger;
use Monolog\Handler\StdoutHandler;
// Create the logger
$logger = new Logger('my_logger');
// Now add some handlers
$logger->pushHandler(new StdoutHandler());

class TestFetchOffset {
	private $groupBroker = null;

	protected function getGroup() {
		$data = array(
			'group_id' => 'test',
		);
		$group = new \Kafka\Protocol\GroupCoordinator('0.9.1.0');
		$requestData = $group->encode($data);
		$socket = new \Kafka\SocketAsyn('10.13.4.159', '9192');
		$socket->SetonReadable(function($data) use($group) {
			$coodid = \Kafka\Protocol\Protocol::unpack(\Kafka\Protocol\Protocol::BIT_B32, substr($data, 0, 4));
			$this->groupBroker = $group->decode(substr($data, 4));
		});
		$socket->connect();
		$socket->write($requestData);
	}

	public function run() {
		$this->getGroup();
		\Amp\repeat(function ($watcherId) {
			if ($this->groupBroker != null) {
				$host = $this->groupBroker['coordinatorHost'];
				$port = $this->groupBroker['coordinatorPort'];
				$data = array(
					'group_id' => 'test',
					'data' => array(
						array(
							'topic_name' => 'test',
							'partitions' => array(0, 1, 2,3,4,5,6,7,8,9)
						)
					),
				);

				$offset = new \Kafka\Protocol\FetchOffset('0.9.0.0');

				$requestData = $offset->encode($data);

				$socket = new \Kafka\SocketAsyn($host, $port);
				$socket->SetonReadable(function($data) use($offset) {
					$coodid = \Kafka\Protocol\Protocol::unpack(\Kafka\Protocol\Protocol::BIT_B32, substr($data, 0, 4));
					var_dump($offset->decode(substr($data, 4)));
				});
				$socket->connect();
				$socket->write($requestData);

				Amp\cancel($watcherId);
			}
		}, $msInterval = 1000);
	}
}


$offset = new TestFetchOffset();
$offset->run();


Amp\run(function () use ($socket, $requestData) {
});
