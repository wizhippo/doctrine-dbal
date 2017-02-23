<?php
/*
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
 * "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
 * LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
 * A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
 * OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
 * SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
 * LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
 * DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
 * THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
 * OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 *
 * This software consists of voluntary contributions made by many individuals
 * and is licensed under the MIT license. For more information, see
 * <http://www.doctrine-project.org>.
 */

namespace Doctrine\DBAL\Driver\Ibase;

use Doctrine\DBAL\Driver\Connection;
use Doctrine\DBAL\Driver\ServerInfoAwareConnection;
use Doctrine\DBAL\TransactionIsolationLevel;
use function addcslashes;
use function func_get_args;
use function ibase_close;
use function ibase_commit;
use function ibase_commit_ret;
use function ibase_connect;
use function ibase_errcode;
use function ibase_errmsg;
use function ibase_pconnect;
use function ibase_query;
use function ibase_rollback;
use function ibase_server_info;
use function is_float;
use function is_int;
use function is_numeric;
use function is_resource;
use function str_replace;

/**
 * @author Douglas Hammond <wizhippo@gmail.com>
 * @author Andreas Prucha, Helicon Software Development <prucha@helicon.co.at>
 */
class IbaseConnection implements Connection, ServerInfoAwareConnection
{
    /**
     * Attribute to set the default transaction isolation level.
     *
     * @see \Doctrine\DBAL\TransactionIsolationLevel
     */
    const ATTR_DOCTRINE_DEFAULT_TRANS_ISOLATION_LEVEL = 'doctrineTransactionIsolationLevel';

    /**
     * Transaction wait timeout (seconds) in case of an locking conflict
     */
    const ATTR_DOCTRINE_DEFAULT_TRANS_WAIT = 'doctrineTransactionWait';

    /**
     * @var resource Connection resource
     */
    private $connectionResource;

    /**
     * @var resource Transaction resource
     */
    private $transactionResource;

    /**
     * @var int Isolation level to use when a transaction is started
     */
    private $attrTransactionIsolationLevel = TransactionIsolationLevel::READ_COMMITTED;

    /**
     * @var int Number of seconds to wait.
     */
    private $attrTransactionWait = 5;

    /**
     * @var boolean
     */
    private $attrAutoCommit = true;

    /**
     * @param array       $params
     * @param string      $username
     * @param string      $password
     * @param string|null $charset
     * @param int|null    $buffers
     * @param int|null    $dialect
     * @param string|null $role
     * @param string|null $sync
     * @param bool        $persistent
     * @param array       $driverOptions
     *
     * @throws \Doctrine\DBAL\Driver\Ibase\IbaseException
     */
    public function __construct(
        array $params,
        $username,
        $password,
        $charset = null,
        $buffers = null,
        $dialect = null,
        $role = null,
        $sync = null,
        $persistent = false,
        array $driverOptions = []
    ) {
        $host = $params['host'] ?? 'localhost';
        $port = $params['port'] ?? 3050;
        $dbname = $params['dbname'] ?? 'example.ib';
        $dbs = $this->formatDbConnString($host, $port, $dbname);
        $this->setDriverOptions($driverOptions);

        $this->connectionResource = $persistent ?
            @ibase_pconnect(
                $dbs,
                $username,
                $password,
                $charset,
                $buffers,
                $dialect,
                $role,
                $sync
            ) : @ibase_connect(
                $dbs,
                $username,
                $password,
                $charset,
                $buffers,
                $dialect,
                $role,
                $sync
            );

        if (!is_resource($this->connectionResource)) {
            $this->checkLastApiCallAndThrowOnError();
        }

        if ($this->attrAutoCommit) {
            $this->beginTransaction();
        }
    }

    /**
     * @throws \Doctrine\DBAL\Driver\Ibase\IbaseException
     */
    public function __destruct()
    {
        $this->doAutoCommitIfEnabled();
        is_resource($this->transactionResource) && $this->rollback();
        @ibase_close($this->connectionResource);
    }

    /**
     * Returns the current transaction resource or false
     *
     * @return resource|false
     */
    public function getTransactionResource()
    {
        return $this->transactionResource;
    }

    /**
     * {@inheritdoc}
     *
     * @throws \UnexpectedValueException if the version string returned by the database server does not contain a
     * parsible version number.
     */
    public function getServerVersion()
    {
        return ibase_server_info($this->connectionResource, IBASE_SVC_SERVER_VERSION);
    }

    /**
     * {@inheritdoc}
     */
    public function requiresQueryForServerVersion()
    {
        return false;
    }

    /**
     * {@inheritDoc}
     *
     * @return \Doctrine\DBAL\Driver\Ibase\IbaseStatement
     */
    public function prepare($prepareString)
    {
        return new IbaseStatement($this, $prepareString);
    }

    /**
     * {@inheritdoc}
     *
     * @throws \Doctrine\DBAL\Driver\Ibase\IbaseException
     */
    public function query()
    {
        $args = func_get_args();
        $sql = $args[0];
        $stmt = $this->prepare($sql);
        $stmt->execute();

        return $stmt;
    }

    /**
     * {@inheritdoc}
     */
    public function quote($value, $type = \PDO::PARAM_STR)
    {
        if (is_int($value) || is_float($value)) {
            return $value;
        }
        $value = str_replace("'", "''", $value);

        return "'" . addcslashes($value, "\000\n\r\\\032") . "'";
    }

    /**
     * {@inheritdoc}
     *
     * @throws \Doctrine\DBAL\Driver\Ibase\IbaseException
     */
    public function exec($statement)
    {
        $stmt = $this->prepare($statement);
        $stmt->execute();

        return $stmt->rowCount();
    }

    /**
     * {@inheritdoc}
     *
     * @throws \Doctrine\DBAL\Driver\Ibase\IbaseException
     */
    public function lastInsertId($name = null)
    {
        if ($name === null) {
            return false;
        }

        $sql = 'SELECT GEN_ID(' . $name . ', 0) LAST_VAL FROM RDB$DATABASE';
        $stmt = $this->query($sql);
        $result = $stmt->fetchColumn(0);

        return $result;
    }

    /**
     * Format a connection string to connect to database
     *
     * @param string $host
     * @param int    $port
     * @param string $dbname
     *
     * @return string
     */
    private function formatDbConnString($host, $port, $dbname)
    {
        if (is_numeric($port)) {
            $port = '/' . (integer)$port;
        }
        if ($dbname) {
            $dbname = ':' . $dbname;
        }

        return $host . $port . $dbname;
    }

    /**
     * @param array $driverOptions
     *
     * @throws \Doctrine\DBAL\Driver\Ibase\IbaseException
     */
    private function setDriverOptions(array $driverOptions = [])
    {
        foreach ($driverOptions as $option => $value) {
            switch ($option) {
                case self::ATTR_DOCTRINE_DEFAULT_TRANS_ISOLATION_LEVEL:
                    $this->attrTransactionIsolationLevel = $value;
                    break;
                case self::ATTR_DOCTRINE_DEFAULT_TRANS_WAIT:
                    $this->attrTransactionWait = $value;
                    break;
                case \PDO::ATTR_AUTOCOMMIT:
                    $this->attrAutoCommit = $value;
                    break;
                default:
                    throw new IbaseException(
                        sprintf("Unsupported option '%s' with value '%s'", $option, $value)
                    );
            }
        }
    }

    /**
     * @param int $isolationLevel
     *
     * @return string
     * @throws \Doctrine\DBAL\Driver\Ibase\IbaseException
     */
    private function getStartTransactionSql($isolationLevel)
    {
        switch ($isolationLevel) {
            case TransactionIsolationLevel::READ_UNCOMMITTED:
                $sql = 'SET TRANSACTION READ WRITE ISOLATION LEVEL READ COMMITTED RECORD_VERSION';
                break;
            case TransactionIsolationLevel::READ_COMMITTED:
                $sql = 'SET TRANSACTION READ WRITE ISOLATION LEVEL READ COMMITTED RECORD_VERSION';
                break;
            case TransactionIsolationLevel::REPEATABLE_READ:
                $sql = 'SET TRANSACTION READ WRITE ISOLATION LEVEL SNAPSHOT';
                break;
            case TransactionIsolationLevel::SERIALIZABLE:
                $sql = 'SET TRANSACTION READ WRITE ISOLATION LEVEL SNAPSHOT TABLE STABILITY';
                break;
            default:
                throw new IbaseException(sprintf("Unsupported transaction isolation level '%i'"));
        }

        if ($this->attrTransactionWait > 0) {
            $sql .= ' WAIT LOCK TIMEOUT ' . $this->attrTransactionWait;
        } elseif ($this->attrTransactionWait === -1) {
            $sql .= ' WAIT';
        } else {
            $sql .= ' NO WAIT';
        }

        return $sql;
    }

    /**
     * {@inheritdoc}
     *
     * @throws \Doctrine\DBAL\Driver\Ibase\IbaseException
     */
    public function beginTransaction()
    {
        if (!is_resource($this->transactionResource)) {
            $this->transactionResource = @ibase_query(
                $this->connectionResource,
                $this->getStartTransactionSql($this->attrTransactionIsolationLevel)
            );
            if (!is_resource($this->transactionResource)) {
                $this->checkLastApiCallAndThrowOnError();
            }
        }

        return true;
    }

    /**
     * {@inheritdoc}
     *
     * @throws \Doctrine\DBAL\Driver\Ibase\IbaseException
     */
    public function commit()
    {
        if (!is_resource($this->transactionResource)) {
            throw new IbaseException('No transaction to commit');
        }

        @ibase_commit($this->transactionResource) || $this->checkLastApiCallAndThrowOnError();
        $this->transactionResource = false;

        return true;
    }

    /**
     * {@inheritdoc}
     *
     * @throws \Doctrine\DBAL\Driver\Ibase\IbaseException
     */
    public function rollback()
    {
        if (!is_resource($this->transactionResource)) {
            throw new IbaseException('No transaction to rollback');
        }

        @ibase_rollback($this->transactionResource) || $this->checkLastApiCallAndThrowOnError();
        $this->transactionResource = false;

        return true;
    }

    /**
     * @throws \Doctrine\DBAL\Driver\Ibase\IbaseException
     */
    public function doAutoCommitIfEnabled()
    {
        if (!$this->attrAutoCommit) {
            return;
        }

        if (!is_resource($this->transactionResource)) {
            throw new IbaseException('No transaction to commit');
        }

        @ibase_commit_ret($this->transactionResource) || $this->checkLastApiCallAndThrowOnError();
    }

    /**
     * {@inheritdoc}
     */
    public function errorCode()
    {
        return ibase_errcode();
    }

    /**
     * {@inheritdoc}
     */
    public function errorInfo()
    {
        $errorCode = $this->errorCode();

        return $errorCode !== false ? [
            'code' => $errorCode,
            'message' => ibase_errmsg()
        ] : ['code' => null, 'message' => null];
    }

    /**
     * @throws \Doctrine\DBAL\Driver\Ibase\IbaseException
     */
    private function checkLastApiCallAndThrowOnError()
    {
        $lastError = $this->errorInfo();
        if ($lastError['code'] !== null) {
            throw IbaseException::fromErrorInfo($lastError);
        }
    }
}
