<?php
// Define path to application directory
define('ROOT_PATH', __DIR__);

// Ensure library/ is on include_path
/*
set_include_path(implode(PATH_SEPARATOR, array(
	realpath(ROOT_PATH . '/library'),
	get_include_path(),
)));
*/
ini_set('display_errors', true);

date_default_timezone_set('Europe/Prague');

require_once ROOT_PATH . '/vendor/autoload.php';
