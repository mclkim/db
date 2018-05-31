<?php

namespace Mcl\Db;

use PDO;
use PDOException;
use PDOStatement;

class DBManager //implements DBManagerInterface
{
    const DB_PARAM_SCALAR = 1;
    const DB_PARAM_OPAQUE = 2;
    const DB_PARAM_MISC = 3;
    const DB_AUTO_INSERT = 1;
    const DB_AUTO_UPDATE = 2;
    const DB_AUTO_REPLACE = 3;
    var $enableLogging = true;
    private $pdo = null;
    private $logger = null;

    function __construct(PDO $pdoInstance = null, $logger = null)
    {
        $this->pdo = $pdoInstance;
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
        $sql = $this->interpolateQuery($query, $bindValues);
        $this->debug($sql);

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
     * Replaces any parameter placeholders in a query with the value of that
     * parameter. Useful for debugging. Assumes anonymous parameters from
     * $params are are in the same order as specified in $query
     *
     * @param string $query The sql query with parameter placeholders
     * @param array $params The array of substitution parameters
     * @return string The interpolated query
     */
    public function interpolateQuery($query, $params)
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

            if (is_array($value))
                $values[$key] = implode(',', $value);

            if (is_null($value))
                $values[$key] = 'NULL';
        }
        // Walk the array to see if we can add single-quotes to strings
        array_walk($values, create_function('&$v, $k', 'if (!is_numeric($v) && $v!="NULL") $v = "\'".$v."\'";'));

        $query = preg_replace($keys, $values, $query, 1, $count);

        return $query;
    }

    protected function debug($message, array $context = array())
    {
        $message = is_array($message) ? var_export($message, true) : $message;
        if ($this->enableLogging && !is_null($this->logger))
            $this->logger->debug($message, $context);
    }

    protected function err($message, array $context = array())
    {
        $message = is_array($message) ? var_export($message, true) : $message;
        if (!is_null($this->logger))
            $this->logger->error($message, $context);
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
        $sql = $this->interpolateQuery($query, $bindValues);
        $this->debug($sql);

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

    private function _executeEmulateQuery($query, $data = array())
    {
        $this->_prepareEmulateQuery($query);

        // $stmt = ( int ) $stmt;
        $data = ( array )$data;
        $this->last_parameters = $data;

        if (count($this->prepare_types) != count($data)) {
            // throw new DB\Exception ( $e->getMessage () );
            return false;
        }

        $realquery = $this->prepare_tokens [0];

        $i = 0;
        foreach ($data as $value) {
            if ($this->prepare_types [$i] == self::DB_PARAM_SCALAR) {
                $realquery .= $this->quote($value);
            } elseif ($this->prepare_types [$i] == self::DB_PARAM_OPAQUE) {
                $fp = @fopen($value, 'rb');
                if (!$fp) {
                    // return $this->raiseError ( DB_ERROR_ACCESS_VIOLATION );
                    // throw new DB\Exception ( $e->getMessage () );
                    return false;
                }
                $realquery .= $this->quote(fread($fp, filesize($value)));
                fclose($fp);
            } else {
                $realquery .= $value;
            }

            $realquery .= $this->prepare_tokens [++$i];
        }

        return $realquery;
    }

    private function _prepareEmulateQuery($query)
    {
        $tokens = preg_split('/((?<!\\\)[&?!])/', $query, -1, PREG_SPLIT_DELIM_CAPTURE);
        $token = 0;
        $types = array();
        $newtokens = array();

        foreach ($tokens as $val) {
            switch ($val) {
                case '&' :
                    $types [$token++] = self::DB_PARAM_OPAQUE;
                    break;
                case '?' :
                    $types [$token++] = self::DB_PARAM_SCALAR;
                    break;
                case '!' :
                    $types [$token++] = self::DB_PARAM_MISC;
                    break;
                default :
                    $newtokens [] = preg_replace('/\\\([&?!])/', "\\1", $val);
            }
        }

        $this->prepare_tokens = &$newtokens;
        $this->prepare_types = $types;
        $this->prepared_queries = implode(' ', $newtokens);

        return $tokens;
    }

    public function quote($string, $type = \PDO::PARAM_STR)
    {
        return $this->pdo->quote($string, $type);
    }
}