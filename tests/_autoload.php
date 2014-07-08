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
function autoload($className)
{
    $basePath = dirname(dirname(__FILE__)) . '/src/tests/';
    $classFile = $basePath . str_replace('\\', DIRECTORY_SEPARATOR, $className) . '.php';
    var_dump($classFile);
    if (file_exists($classFile)) {
        require_once $classFile;
        return true;
    }
//    var_dump($classFile);
//    If (function_exists('stream_resolve_include_path')) {
//        $file = stream_resolve_include_path($classFile);
//    } else {
//        foreach (explode(PATH_SEPARATOR, get_include_path()) as $path) {
//            if (file_exists($path . '/' . $classFile)) {
//                $file = $path . '/' . $classFile;
//                break;
//            }
//        }
//    }
//    /* If file is found, store it into the cache, classname <-> file association */
//    if (($file !== false) && ($file !== null)) {
//        include $file;
//        return;
//    }

    return false;
}
