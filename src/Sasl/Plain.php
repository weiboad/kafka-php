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
use Kafka\SaslMechanism;
use Kafka\Exception;

/**
+------------------------------------------------------------------------------
* Kafka sasl provider for plain mechanism
+------------------------------------------------------------------------------
*
* @package
* @version $_SWANBR_VERSION_$
* @copyright Copyleft
* @author $_SWANBR_AUTHOR_$
+------------------------------------------------------------------------------
*/

final class Plain extends Mechanism implements SaslMechanism
{
    // {{{ consts
    
    private const MECHANISM_NAME = "PLAIN";

    // }}}
    // {{{ members

    private $username;

    private $password;

    // }}}
    // {{{ functions
    // {{{ public function __construct()
    
    /**
     *
     * __construct
     *
     * @access public
     * @return void
     */
    public function __construct(string $username, string $password)
    {
        $this->username = trim($username);
        $this->password = trim($password);
    }

    // }}}
    // {{{ public function authenticate()
    
    /**
     *
     * sasl authenticate
     *
     * @access public
     * @return void
     */
    public function authenticate(CommonSocket $socket) : void
    {
        $this->handShake($socket, $this->getMechanismName());
        $split = \Kafka\Protocol\Protocol::pack(\Kafka\Protocol\Protocol::BIT_B8, 0);
        $data  = $split . $this->username . $split . $this->password;
        $data  = \Kafka\Protocol\Protocol::encodeString($data, \Kafka\Protocol\Protocol::PACK_INT32);
        $socket->writeBlocking($data);
        $data = $socket->readBlocking(4, true);
    }

    // }}}
    // {{{ public function getMechanismName()
    
    /**
     *
     * get sasl authenticate mechanism name
     *
     * @access public
     * @return string
     */
    public function getMechanismName() : string
    {
        return self::MECHANISM_NAME;
    }

    // }}}
    // }}}
}
