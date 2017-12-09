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

namespace KafkaTest\Base;

/**
+------------------------------------------------------------------------------
* Kafka protocol since Kafka v0.8
+------------------------------------------------------------------------------
*
* @package
* @version $_SWANBR_VERSION_$
* @copyright Copyleft
* @author $_SWANBR_AUTHOR_$
+------------------------------------------------------------------------------
*/

class SaslGssapiTest extends \PHPUnit\Framework\TestCase
{
    // {{{ consts
    // }}}
    // {{{ members

    private $keytab;

    // }}}
    // {{{ functions
    // {{{ protected function setUp()

    protected function setUp()
    {
        $this->keytab = dirname(__FILE__) . '/id/testkeytab';
        $this->initKeytab();
    }

    // }}}
    // {{{ protected function tearDown()

    protected function tearDown()
    {
        \uopz_unset_return('extension_loaded');
        \uopz_unset_return('is_readable');
        \uopz_set_return(\KRB5CCache::class, 'initKeytab', '');
        $this->deleteKeytab();
    }

    // }}}
    // {{{ public function testExtensionLoaded()

    /**
     * testExtensionLoaded
     *
     * @expectedException \Kafka\Exception
     * @expectedExceptionMessage Extension `krb5` not installed.
     * @access public
     * @return void
     */
    public function testExtensionLoaded()
    {
        // modify `extension_loaded` return false
        \uopz_set_return('extension_loaded', false);

        $socket   = $this->createMock(\Kafka\Socket::class);
        $provider = new \Kafka\Sasl\Gssapi($this->keytab, 'testprincipal');
        $provider->authenticate($socket);
    }

    // }}}
    // {{{ public function testGssapi()

    /**
     * testGssapi
     *
     * @access public
     * @return void
     */
    public function testGssapi()
    {
        $principal = 'testprincipal';
        $socket    = $this->getSocketForTestGssapi();
        

        // ZEND_ACC_CLASS == 0
        \uopz_flags(\Kafka\Sasl\Gssapi::class, null, 0);
        \uopz_flags(\Kafka\Sasl\Gssapi::class, 'initSecurityContext', ZEND_ACC_PUBLIC);
        \uopz_flags(\Kafka\Sasl\Gssapi::class, 'warpToken', ZEND_ACC_PUBLIC);

        $provider = $this->getMockBuilder(\Kafka\Sasl\Gssapi::class)
                         ->setMethods(['initSecurityContext', 'warpToken'])
                         ->setConstructorArgs([$this->keytab, $principal])
                         ->getMock();

        $token = \hex2bin('6082024e06092a864886f71201020201006e82023d30820239a003020105a10302010ea20703050000000000a3820147618201433082013fa003020105a1101b0e4e4d5245444b41464b412e434f4da2193017a003020101a110300e1b056b61666b611b056e6f646531a382010930820105a003020112a103020102a281f80481f597006a24dff024c2bdce31208d10baf56f5d350701c4b67cc51f49cab447d5e472a68dfc00f8cf224f5c2bfb32cd2e22eb02f36ede0200288c97812668370ee4770ff7d8b3bd0e5317c5c03e16b9b15c6a3e74278486ca8a5fc6662d0423d20db556bd57cf6666146ac8e947add40bbfa0d95fed9f028b693a826b707b70d4570b86ac5b608f2e11148c7f7d16136581c02021cb4a33716351ad49835d0ff42335c8e468be49599ec3c4885a91a0957d03163bdc602f5727eefb5c349a9c4108a7b45f82c1daba5eb422f28e99b9488078cf126097077187cca19d2b9938bf13f70ecbc32f69dd694ace256f5bf4553853a91cb91ca481d83081d5a003020112a281cd0481ca3a4b55c8b95e41b8337e0f95d2b26289ce3db0793d0345961271bb3c7beff0f2a354110c5e6edab02b932568baa19ba340b811aca8a0e7e750962f2d2bf8d2144d7e9de9f3826daaa11d9a4a2f0b8880f5b3ab924b4a6e6f8b93a92d54fc085f9f921c8b98487942f29724457407f94f9b010fff2928b04e648fcd13b830417db3f9ae1cc13c03d6b0d660ceba0730f00ea161a7a83141a56e19734dee2825952e7fc6a03b8bf3eee5693b99ac63d34bd4593dfdecb29628d4d54eb343a43c95637595e56fbd1bcacab4');
        $provider->method('initSecurityContext')
                 ->will($this->returnValue($token));
        $provider->method('warpToken')
                 ->with($this->equalTo(\hex2bin('050401ff000c0000000000003661d1c10101000011e9d2da795b1800cdf2ffc7')))
                 ->will($this->returnValue(\hex2bin('050400ff000c0000000000003661d1c1050401ff000c0000000000003661d1c10101000011e9d2da795b1800cdf2ffc79eccf0bb0e3a15a5164711a0')));

        $provider->authenticate($socket);
    }

    // }}}
    // {{{ public function testKeytabIsNotExists()

    /**
     * testKeytabIsNotExists
     *
     * @expectedException \Kafka\Exception
     * @expectedExceptionMessage Invalid keytab, keytab file not exists.
     * @access public
     * @return void
     */
    public function testKeytabIsNotExists()
    {
        new \Kafka\Sasl\Gssapi($this->keytab . 'rand', 'testprincipal');
    }

    // }}}
    // {{{ public function testKeytabIsNotReadable()

    /**
     * testKeytabIsNotReadable
     *
     * @expectedException \Kafka\Exception
     * @expectedExceptionMessage Invalid keytab, keytab file disable read.
     * @access public
     * @return void
     */
    public function testKeytabIsNotReadable()
    {
        // modify `is_readable` return false
        \uopz_set_return('is_readable', false);
        new \Kafka\Sasl\Gssapi($this->keytab, 'testprincipal');
    }

    // }}}
    // {{{ public function testGetMechanismName()

    /**
     * testGetMechanismName
     *
     * @access public
     * @return void
     */
    public function testGetMechanismName()
    {
        $provider = new \Kafka\Sasl\Gssapi($this->keytab, 'testprincipal');
        $this->assertSame('GSSAPI', $provider->getMechanismName());
    }

    // }}}
    // {{{ public function testInitSecurityContext()

    /**
     * testInitSecurityContext
     *
     * @access public
     * @return void
     */
    public function testInitSecurityContext()
    {
        if (! extension_loaded('krb5')) {
            return;
        }
        \uopz_flags(\Kafka\Sasl\Gssapi::class, 'initSecurityContext', ZEND_ACC_PUBLIC);
        $this->mockKrb5(true);
        $provider = new \Kafka\Sasl\Gssapi($this->keytab, 'testprincipal');
        $this->assertSame('', $provider->initSecurityContext());
    }

    // }}}
    // {{{ public function testInitSecurityContextNotSuccess()

    /**
     * testInitSecurityContext
     *
     * @expectedException \Kafka\Exception
     * @expectedExceptionMessage Init security context failure.
     * @access public
     * @return void
     */
    public function testInitSecurityContextNotSuccess()
    {
        if (! extension_loaded('krb5')) {
            return;
        }
        \uopz_flags(\Kafka\Sasl\Gssapi::class, 'initSecurityContext', ZEND_ACC_PUBLIC);
        $this->mockKrb5(false);
        $provider = new \Kafka\Sasl\Gssapi($this->keytab, 'testprincipal');
        $provider->initSecurityContext();
    }

    // }}}
    // {{{ public function testWarpToken()

    /**
     * testWarpToken
     *
     * @access public
     * @return void
     */
    public function testWarpToken()
    {
        if (! extension_loaded('krb5')) {
            return;
        }
        \uopz_flags(\Kafka\Sasl\Gssapi::class, 'warpToken', ZEND_ACC_PUBLIC);
        $this->mockKrb5(true);
        $provider = new \Kafka\Sasl\Gssapi($this->keytab, 'testprincipal');
        $provider->initSecurityContext();
        $this->assertSame('', $provider->warpToken('xxxx'));
    }

    // }}}
    // {{{ private function getSocketForTestGssapi()

    private function getSocketForTestGssapi()
    {
        $socket        = $this->createMock(\Kafka\Socket::class);
        $handShakeData = \hex2bin('00000011000000000004000d534352414d2d5348412d3531320005504c41494e0006475353415049000d534352414d2d5348412d323536');
        $stokenLength  = \hex2bin('00000020');
        $stokenData    = \hex2bin('050401ff000c0000000000003661d1c10101000011e9d2da795b1800cdf2ffc7');
        $socket->method('readBlocking')
            ->will($this->onConsecutiveCalls(
                // hand shake response data length
                    \hex2bin('00000037'),
                $handShakeData,
                $stokenLength,
                $stokenData
            ));

        $writeToken = \hex2bin('000002526082024e06092a864886f71201020201006e82023d30820239a003020105a10302010ea20703050000000000a3820147618201433082013fa003020105a1101b0e4e4d5245444b41464b412e434f4da2193017a003020101a110300e1b056b61666b611b056e6f646531a382010930820105a003020112a103020102a281f80481f597006a24dff024c2bdce31208d10baf56f5d350701c4b67cc51f49cab447d5e472a68dfc00f8cf224f5c2bfb32cd2e22eb02f36ede0200288c97812668370ee4770ff7d8b3bd0e5317c5c03e16b9b15c6a3e74278486ca8a5fc6662d0423d20db556bd57cf6666146ac8e947add40bbfa0d95fed9f028b693a826b707b70d4570b86ac5b608f2e11148c7f7d16136581c02021cb4a33716351ad49835d0ff42335c8e468be49599ec3c4885a91a0957d03163bdc602f5727eefb5c349a9c4108a7b45f82c1daba5eb422f28e99b9488078cf126097077187cca19d2b9938bf13f70ecbc32f69dd694ace256f5bf4553853a91cb91ca481d83081d5a003020112a281cd0481ca3a4b55c8b95e41b8337e0f95d2b26289ce3db0793d0345961271bb3c7beff0f2a354110c5e6edab02b932568baa19ba340b811aca8a0e7e750962f2d2bf8d2144d7e9de9f3826daaa11d9a4a2f0b8880f5b3ab924b4a6e6f8b93a92d54fc085f9f921c8b98487942f29724457407f94f9b010fff2928b04e648fcd13b830417db3f9ae1cc13c03d6b0d660ceba0730f00ea161a7a83141a56e19734dee2825952e7fc6a03b8bf3eee5693b99ac63d34bd4593dfdecb29628d4d54eb343a43c95637595e56fbd1bcacab4');
        $message    = \hex2bin('0000003c050400ff000c0000000000003661d1c1050401ff000c0000000000003661d1c10101000011e9d2da795b1800cdf2ffc79eccf0bb0e3a15a5164711a0');
        $socket->expects($this->exactly(3))
            ->method('writeBlocking')
            ->withConsecutive(
                // write handshake request
                [$this->equalTo(\hex2bin('0000001b001100000000001100096b61666b612d7068700006475353415049'))],
                [$this->equalTo($writeToken)],
                [$this->equalTo($message)]
            );
        return $socket;
    }
    // }}}
    // {{{ private function deleteKeytab()

    private function deleteKeytab()
    {
        if (file_exists($this->keytab)) {
            unlink($this->keytab);
        }
        if (file_exists(dirname($this->keytab))) {
            rmdir(dirname($this->keytab));
        }
    }
    // }}}
    // {{{ private function initKeytab()

    private function initKeytab()
    {
        $this->deleteKeytab();
        mkdir(dirname($this->keytab));
        file_put_contents($this->keytab, 'test');
    }

    // }}}
    // {{{ private function mockKrb5()

    private function mockKrb5($success = false)
    {
        \uopz_set_return(\KRB5CCache::class, 'initKeytab', '');
        \uopz_set_return(\GSSAPIContext::class, 'acquireCredentials', '');
        \uopz_set_return(\GSSAPIContext::class, 'initSecContext', $success);
        \uopz_set_return(\GSSAPIContext::class, 'wrap', '');
    }

    // }}}
    // }}}
}
