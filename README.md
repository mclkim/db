# Database for PHP

## Description
Safe and convenient SQL database access in a driver-agnostic way

## Prerequisites

[PHP](http://php.net/)
```
$ php -v
PHP 5.5.X (cli) (built: Jul  6 2017 16:51:52) ( ZTS MSVC14 (Visual C++ 2015) x64 )
Copyright (c) 1997-2017 The PHP Group
Zend Engine v3.1.0, Copyright (c) 1998-2017 Zend Technologies
    with Zend OPcache v7.1.7, Copyright (c) 1999-2017, by Zend Technologies
```
[Composer](https://getcomposer.org/)
```
$ composer --version
Composer version 1.5.1 2017-08-09 16:07:22
```

## 1.Install
First, at the command line, make working directory:
```
$ mkdir homepage
$ cd homepage
```
and require the necessary libraries:
```
$ composer require mclkim/db:dev-master
```

## 2.Example copy on local development
The following is a working example. 
```
$ cp -rf vendor/mclkim/db/example/* .
```

## 3.Web brower
You can test the framework using the [public/index.php](public/index.php)
example. You can run the demo using the internal web server of PHP with the
following command:
```
$ php -S 0.0.0.0:8000 -t public/
```
... and point your browser to http://localhost:8000/ 
```
http://localhost:8000/
```
## Reference
 * [PHP-DB](https://github.com/delight-im/PHP-DB)
 * [Pixie Query Builder](https://github.com/usmanhalalit/pixie)
 * [DataGrid ](https://github.com/contributte/datagrid)
 
Released under the [MIT License](LICENSE)