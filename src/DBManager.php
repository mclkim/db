<?php

namespace Mcl\Db;

use PDO;
use PDOException;
use PDOStatement;
use Psr\Log\LoggerAwareInterface;
use Psr\Log\LoggerInterface;

class DBManager implements LoggerAwareInterface //implements DBManagerInterface
{
    const DB_AUTO_INSERT = 1;
    const DB_AUTO_UPDATE = 2;
    const DB_AUTO_REPLACE = 3;
    var $enableLogging = true;
    private $pdo = null;
    private $logger = null;

    function __construct(PDO $pdo = null, $logger = null)
    {
        if ($pdo) {
            $this->pdo = $pdo;
        }
        $this->logger = $logger;
    }


    public function setLogger(LoggerInterface $logger)
    {
        $this->logger = $logger;
    }

    public function getPdo()
    {
        return $this->pdo;
    }

    function version()
    {
        return $this->executePreparedQueryToMap('select version()');
    }

    function executePreparedQueryToMap($query, array $bindValues = null)
    {
        return $this->executePreparedQuery(function ($stmt) {
            /** @var PDOStatement $stmt */
            return $stmt->fetch(\PDO::FETCH_ASSOC);
        }, $query, $bindValues);
    }

    private function executePreparedQuery(callable $callback, $query, array $bindValues = null)
    {
//        $sql = $this->executeEmulateQuery($query, $bindValues);
        $fullQuery = $this->interpolateQuery($query, $bindValues);
        $this->debug($fullQuery);

        // create a prepared statement from the supplied SQL string
        try {
            /**
             * TODO::
             * $stmt = $this->pdo->prepare($query);
             */
            $stmt = $this->pdo->prepare($query);
        } catch (PDOException $e) {
            $this->err($e->getMessage());
            ErrorHandler::rethrow($e);
        }

        // bind the supplied values to the query and execute it
        try {
            /**
             * TODO::
             * PHP Warning: PDOStatement::execute(): SQLSTATE[HY093]: Invalid parameter number: parameter was not defined
             * $stmt->execute($bindValues);
             */
            $stmt->execute($bindValues);
        } catch (PDOException $e) {
            $this->err($e->getMessage());
            ErrorHandler::rethrow($e);
        }

        // fetch the desired results from the result set via the supplied callback
        $results = $callback($stmt);

        // if the result is empty
        if (empty($results) && $stmt->rowCount() === 0) {
            // consistently return `null`
            return null;
        } // if some results have been found
        else {
            // return these as extracted by the callback
            return $results;
        }
    }

    /**
     * https://code-examples.net/ko/q/33684
     * https://github.com/noahheck/E_PDOStatement
     * @param string $query
     * @param array $params
     * @return string
     * @throws Exception
     */
    protected function interpolateQuery($query, $params)
    {
        $ps = preg_split("/'/is", $query);
        $pieces = [];
        $prev = null;
        foreach ($ps as $p) {
            $lastChar = substr($p, strlen($p) - 1);

            if ($lastChar != "\\") {
                if ($prev === null) {
                    $pieces[] = $p;
                } else {
                    $pieces[] = $prev . "'" . $p;
                    $prev = null;
                }
            } else {
                $prev .= ($prev === null ? '' : "'") . $p;
            }
        }

        $arr = [];
        $indexQuestionMark = -1;
        $matches = [];

        for ($i = 0; $i < count($pieces); $i++) {
            if ($i % 2 !== 0) {
                $arr[] = "'" . $pieces[$i] . "'";
            } else {
                $st = '';
                $s = $pieces[$i];
                while (!empty($s)) {
                    if (preg_match("/(\?|:[A-Z0-9_\-]+)/is", $s, $matches, PREG_OFFSET_CAPTURE)) {
                        $index = $matches[0][1];
                        $st .= substr($s, 0, $index);
                        $key = $matches[0][0];
                        $s = substr($s, $index + strlen($key));

                        if ($key == '?') {
                            $indexQuestionMark++;
                            if (array_key_exists($indexQuestionMark, $params)) {
                                $st .= $this->quote($params[$indexQuestionMark]);
                            } else {
                                throw new Exception('Wrong params in query at ' . $index);
                            }
                        } else {
                            if (array_key_exists($key, $params)) {
                                $st .= $this->quote($params[$key]);
                            } else {
                                throw new Exception('Wrong params in query with key ' . $key);
                            }
                        }
                    } else {
                        $st .= $s;
                        $s = null;
                    }
                }
                $arr[] = $st;
            }
        }

        return implode('', $arr);
    }

    public function quote($str)
    {
        if (!is_array($str)) {
            return $this->pdo->quote($str);
        } else {
            $str = implode(',', array_map(function ($v) {
                return $this->quote($v);
            }, $str));

            if (empty($str)) {
                return 'NULL';
            }

            return $str;
        }
    }

    protected function debug($message, array $context = array())
    {
        if ($this->enableLogging && $this->logger) {
            $message = is_array($message) ? var_export($message, true) : $message;
            $this->logger->debug($message, $context);
        }
    }

    protected function err($message, array $context = array())
    {
        if (!is_null($this->logger)) {
            $message = is_array($message) ? var_export($message, true) : $message;
            $this->logger->error($message, $context);
        }
    }

    function executePreparedQueryOne($query, array $bindValues = null)
    {
        return $this->executePreparedQuery(function ($stmt) {
            /** @var PDOStatement $stmt */
            return $stmt->fetch(\PDO::FETCH_COLUMN);
        }, $query, $bindValues);
    }

    function executePreparedQueryToMapList($query, array $bindValues = null)
    {
        return $this->executePreparedQuery(function ($stmt) {
            /** @var PDOStatement $stmt */
            return $stmt->fetchAll(\PDO::FETCH_ASSOC);
        }, $query, $bindValues);
    }

    function executePreparedQueryToArrayList($query, array $bindValues = null)
    {
        return $this->executePreparedQuery(function ($stmt) {
            /** @var PDOStatement $stmt */
            return $stmt->fetchAll(\PDO::FETCH_NUM);
        }, $query, $bindValues);
    }

    function executePreparedQueryToObjList($query, array $bindValues = null)
    {
        return $this->executePreparedQuery(function ($stmt) {
            /** @var PDOStatement $stmt */
            return $stmt->fetchAll(\PDO::FETCH_OBJ);
        }, $query, $bindValues);
    }

    public function startTransaction()
    {
        $this->beginTransaction();
    }

    public function beginTransaction()
    {
        try {
            $success = $this->pdo->beginTransaction();
        } catch (PDOException $e) {
            $success = $e->getMessage();
        }

        if ($success !== true) {
            throw new \Exception(is_string($success) ? $success : null);
        }
    }

    public function isTransactionActive()
    {
        $state = $this->pdo->inTransaction();
        return $state;
    }

    public function commit()
    {
        try {
            $success = $this->pdo->commit();
        } catch (PDOException $e) {
            $success = $e->getMessage();
        }

        if ($success !== true) {
            throw new \Exception(is_string($success) ? $success : null);
        }
    }

    public function rollBack()
    {
        try {
            $success = $this->pdo->rollBack();
        } catch (PDOException $e) {
            $success = $e->getMessage();
        }

        if ($success !== true) {
            throw new \Exception(is_string($success) ? $success : null);
        }
    }

    function executeTransaction()
    {
        try {
            // Begin the PDO transaction
            $this->pdo->beginTransaction();

            // If no errors have been thrown or the transaction wasn't completed within
            // the closure, commit the changes
            $this->pdo->commit();

            return $this;
        } catch (PDOException $e) {
            // something happened, rollback changes
            ErrorHandler::rethrow($e);
            $this->pdo->rollBack();
            return $this;
        }
    }

    function AutoExecuteInsert($table_name, $fields_values, $where = false)
    {
        $ret = $this->_buildManipSQL($table_name, $fields_values, self::DB_AUTO_INSERT, $where);
        return $this->executePreparedUpdate($ret ['query'], $ret ['params']);
    }

    private function _buildManipSQL($table, $table_fields, $mode, $where = false)
    {
        if (count($table_fields) == 0) {
            return false;
        }

        $fields = [];
        $values = [];
        $qs  = [];

        foreach ($table_fields as $field => $value) {
            $qs[] = '?';
            $fields [] = $field;
            $values [] = $value;
        }

        switch ($mode) {
            case self::DB_AUTO_INSERT :
                $fields = implode(',', $fields);
                $qs = implode(',', $qs);
                $query = "INSERT INTO $table ($fields) VALUES ($qs)";
                return array(
                    'query' => $query,
                    'params' => $values
                );
            case self::DB_AUTO_UPDATE :
                $set = implode('=?,', $fields) . '=?';
                $query = "UPDATE $table SET $set";
                if ($where) {
                    $query .= " WHERE $where";
                }
                return array(
                    'query' => $query,
                    'params' => $values
                );
            case self::DB_AUTO_REPLACE :
                $fields = implode(',', $fields);
                $qs = implode(',', $qs);
                $query = "REPLACE INTO $table ($fields) VALUES ($qs)";
                return array(
                    'query' => $query,
                    'params' => $values
                );
            default :
                return false;
        }
    }

    function executePreparedUpdate($query, array $bindValues = null)
    {
//        $sql = $this->executeEmulateQuery($query, $bindValues);
        $fullQuery = $this->interpolateQuery($query, $bindValues);
        $this->debug($fullQuery);

        // create a prepared statement from the supplied SQL string
        try {
            /**
             * TODO::
             * $stmt = $this->pdo->prepare($query);
             */
            $stmt = $this->pdo->prepare($query);
        } catch (PDOException $e) {
            $this->err($e->getMessage());
            ErrorHandler::rethrow($e);
        }

        // bind the supplied values to the query and execute it
        try {
            /**
             * TODO::
             * PHP Warning: PDOStatement::execute(): SQLSTATE[HY093]: Invalid parameter number: parameter was not defined
             * $stmt->execute($bindValues);
             * */
            $stmt->execute($bindValues);
        } catch (PDOException $e) {
            $this->err($e->getMessage());
            ErrorHandler::rethrow($e);
        }

        $lastInsertId = $this->getLastInsertId();
        return $lastInsertId === '0' ? $stmt->rowCount() : $lastInsertId;
    }

    public function getLastInsertId($sequenceName = null)
    {
        return $id = $this->pdo->lastInsertId($sequenceName);
    }

    function AutoExecuteUpdate($table_name, $fields_values, $where = false)
    {
        $ret = $this->_buildManipSQL($table_name, $fields_values, self::DB_AUTO_UPDATE, $where);
        return $this->executePreparedUpdate($ret ['query'], $ret ['params']);
    }

    function AutoExecuteReplace($table_name, $fields_values, $where = false)
    {
        $ret = $this->_buildManipSQL($table_name, $fields_values, self::DB_AUTO_REPLACE, $where);
        return $this->executePreparedUpdate($ret ['query'], $ret ['params']);
    }

    function getData()
    {
    }

    function getList()
    {
    }

    function getMapList()
    {
    }

    function putData()
    {
    }

    function delData()
    {
    }

    /**
     * Replaces any parameter placeholders in a query with the value of that
     * parameter. Useful for debugging. Assumes anonymous parameters from
     * $params are are in the same order as specified in $query
     *
     * Reference: http://stackoverflow.com/a/1376838/656489
     *
     * @param string $query The sql query with parameter placeholders
     * @param array $params The array of substitution parameters
     *
     * @return string The interpolated query
     */
    protected function getRawSql($query, $params)
    {
        $keys = array();
        $values = $params;

        # build a regular expression for each parameter
        foreach ($params as $key => $value) {
            if (is_string($key)) {
                $keys[] = '/:' . $key . '/';
            } else {
                $keys[] = '/[?]/';
            }

            if (is_string($value)) {
                $values[$key] = $this->pdo->quote($value);
            }

            if (is_array($value)) {
                $values[$key] = implode(',', $this->pdo->quote($value));
            }

            if (is_null($value)) {
                $values[$key] = 'NULL';
            }
        }

        $query = preg_replace($keys, $values, $query, 1, $count);

        return $query;
    }

    protected function info($message = null, array $context = array())
    {
        $message = is_array($message) ? var_export($message, true) : $message;
        if (!is_null($this->logger))
            $this->logger->info($message, $context);
    }

    private function error($message, $context = array())
    {
        if ($this->logger) {
            $this->logger->error($message, $context);
        }
    }


}