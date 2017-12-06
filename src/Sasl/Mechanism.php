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

namespace Kafka\Sasl;

use Kafka\CommonSocket;
use Kafka\Exception;
use Kafka\Protocol;
use Kafka\Protocol\Protocol as ProtocolTool;

/**
+------------------------------------------------------------------------------
* Kafka sasl provider mechanism abstract
+------------------------------------------------------------------------------
*
* @package
* @version $_SWANBR_VERSION_$
* @copyright Copyleft
* @author $_SWANBR_AUTHOR_$
+------------------------------------------------------------------------------
*/

abstract class Mechanism
{
    // {{{ consts
    // }}}
    // {{{ functions
    // {{{ public function handShake()
    
    /**
     *
     * sasl authenticate hand shake
     *
     * @access public
     * @return void
     */
    protected function handShake(CommonSocket $socket, string $mechanism)
    {
        $requestData = Protocol::encode(\Kafka\Protocol::SASL_HAND_SHAKE_REQUEST, $mechanism);
        $socket->selectWrite($requestData);
        $dataLen       = ProtocolTool::unpack(\Kafka\Protocol\Protocol::BIT_B32, $socket->selectRead(4));
        $data          = $socket->selectRead($dataLen);
        $correlationId = ProtocolTool::unpack(\Kafka\Protocol\Protocol::BIT_B32, substr($data, 0, 4));
        $result        = Protocol::decode(\Kafka\Protocol::SASL_HAND_SHAKE_REQUEST, substr($data, 4));
        if (! is_array($result) || ! isset($result['errorCode'])) {
            throw new Exception('Sasl request hand shake response error.');
        }

        if ($result['errorCode'] !== Protocol::NO_ERROR) {
            throw new Exception(Protocol::getError($result['errorCode']));
        }
    }

    // }}}
    // }}}
}
