<?php
require '../../vendor/autoload.php';

class CommitOffset {
	protected $group = array();
	// {{{ functions
	// {{{ protected function joinGroup()
	
	protected function joinGroup() {
		$data = array(
			'group_id' => 'test',
			'session_timeout' => 6000,
			'rebalance_timeout' => 6000,
			'member_id' => '',
			'data' => array(
				array(
					'protocol_name' => 'group',
					'version' => 0,
					'subscription' => array('test'),
					'user_data' => '',
				),
			),
		);

		$protocol = \Kafka\Protocol::init('0.9.1.0');
		$requestData = \Kafka\Protocol::encode(\Kafka\Protocol::JOIN_GROUP_REQUEST, $data);

		$socket = new \Kafka\Socket('127.0.0.1', '9192');
		$socket->SetonReadable(function($data) {
			$coodid = \Kafka\Protocol\Protocol::unpack(\Kafka\Protocol\Protocol::BIT_B32, substr($data, 0, 4));
			$result = \Kafka\Protocol::decode(\Kafka\Protocol::JOIN_GROUP_REQUEST, substr($data, 4));
			$this->group = $result;
			Amp\stop();
		});

		$socket->connect();
		$socket->write($requestData);
		Amp\run(function () use ($socket, $requestData) {
		});
	}

	// }}}
	// {{{ protected function syncGroup()
	
	protected function syncGroup() {
		$this->joinGroup();	
		$data = array(
			'group_id' => 'test',
			'generation_id' => $this->group['generationId'],
			'member_id' => $this->group['memberId'],
			'data' => array(
				array(
					'version' => 0,
					'member_id' => $this->group['memberId'],
					'assignments' => array(
						array(
							'topic_name' => 'test',
							'partitions' => array(
								0
							),
						),
					),
				),
			),
		);

		$protocol = \Kafka\Protocol::init('0.9.1.0');
		$requestData = \Kafka\Protocol::encode(\Kafka\Protocol::SYNC_GROUP_REQUEST, $data);

		$socket = new \Kafka\Socket('127.0.0.1', '9192');
		$socket->SetonReadable(function($data) {
			$coodid = \Kafka\Protocol\Protocol::unpack(\Kafka\Protocol\Protocol::BIT_B32, substr($data, 0, 4));
			$result = \Kafka\Protocol::decode(\Kafka\Protocol::SYNC_GROUP_REQUEST, substr($data, 4));
			//echo json_encode($result);
			Amp\stop();
		});

		$socket->connect();
		$socket->write($requestData);
		Amp\run(function () use ($socket, $requestData) {
		});
	}

	// }}}
	// {{{ public function run()
	
	public function run() {
		$this->joinGroup();	
		$this->syncGroup();	
		$data = array(
			'group_id' => 'test',
			'generation_id' => $this->group['generationId'],
			'member_id' => $this->group['memberId'],
			'retention_time' => 36000,
			'data' => array(
				array(
					'topic_name' => 'test',
					'partitions' => array(
						array(
							'partition' => 0,
							'offset' => 45,
							'metadata' => '',
						)
					)
				)
			),
		);

		$protocol = \Kafka\Protocol::init('0.9.1.0');
		$requestData = \Kafka\Protocol::encode(\Kafka\Protocol::OFFSET_COMMIT_REQUEST, $data);

		$socket = new \Kafka\Socket('127.0.0.1', '9192');
		$socket->SetonReadable(function($data) {
			$coodid = \Kafka\Protocol\Protocol::unpack(\Kafka\Protocol\Protocol::BIT_B32, substr($data, 0, 4));
			$result = \Kafka\Protocol::decode(\Kafka\Protocol::OFFSET_COMMIT_REQUEST, substr($data, 4));
			echo json_encode($result);
			Amp\stop();
		});

		$socket->connect();
		$socket->write($requestData);
		Amp\run(function () use ($socket, $requestData) {
		});
	}

	// }}}
	// }}}
}

$commit = new CommitOffset();
$commit->run();

