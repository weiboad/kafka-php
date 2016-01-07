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

/**
+------------------------------------------------------------------------------
* 安装自动加载器
+------------------------------------------------------------------------------
*
* @package
* @version $_SWANBR_VERSION_$
* @copyright $_SWANBR_COPYRIGHT_$
* @author $_SWANBR_AUTHOR_$
+------------------------------------------------------------------------------
*/
include_once __DIR__ . '/../vendor/autoload.php';

spl_autoload_register('autoload');
/**
 * @param $className
 * @return bool
 */
function autoload($className)
{
    $basePath = dirname(dirname(__FILE__)) . '/tests/';
    $classFile = $basePath . str_replace('\\', DIRECTORY_SEPARATOR, $className) . '.php';
    if (file_exists($classFile)) {
        require_once $classFile;
        return true;
    }

    return false;
}
