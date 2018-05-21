<?php
require __DIR__ . '/../vendor/autoload.php';

$dbname = 'mysql';
$user = 'root';
$pass = 'root1234';
try {
    $pdo = new PDO('mysql:host=localhost;dbname=' . $dbname, $user, $pass);
} catch (PDOException $e) {
    die('Connection failed.' . $e->getMessage());
}

$db = new Mcl\Db\DBManager ($pdo);
var_dump($db->executePreparedQueryOne('select version1()'));