<?php
namespace Kafka\Contracts\Config;

interface Ssl
{
    public function getLocalCert() : string;

    public function getLocalPk() : string;

    public function getVerifyPeer() : bool;

    public function getPassphrase() : string;

    public function getCafile() : string;

    public function getPeerName() : string;
}
