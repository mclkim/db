<?php
/**
 * Created by PhpStorm.
 * User: 김명철
 * Date: 2018-04-22
 * Time: 오전 8:05
 */

namespace Mcl\Db;


interface DBManagerInterface
{
    function getData();

    function getList();

    function getMapList();

    function putData();

    function delData();
}