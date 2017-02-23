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

use Doctrine\DBAL\Driver\Statement;
use Doctrine\DBAL\Driver\StatementIterator;
use Doctrine\DBAL\FetchMode;
use function array_key_exists;
use function array_merge;
use function array_unshift;
use function array_values;
use function call_user_func_array;
use function func_get_args;
use function ibase_affected_rows;
use function ibase_errcode;
use function ibase_errmsg;
use function ibase_fetch_assoc;
use function ibase_fetch_object;
use function ibase_fetch_row;
use function ibase_free_query;
use function ibase_free_result;
use function ibase_num_fields;
use function is_array;
use function is_numeric;
use function is_object;
use function is_resource;
use function is_string;

/**
 * @author Douglas Hammond <wizhippo@gmail.com>
 * @author Andreas Prucha, Helicon Software Development <prucha@helicon.co.at>
 */
class IbaseStatement implements \IteratorAggregate, Statement
{
    const FETCH_FLAG = IBASE_TEXT;

    /**
     * @var IbaseConnection
     */
    private $connection;

    /**
     * @var resource|null Resource of the prepared statement
     */
    private $statementResource;

    /**
     * @var resource|bool|int Query result resource, success, count
     */
    private $resultResource;

    /**
     * @var array
     */
    private $paramBindings = [];

    /**
     * @var array
     */
    private $paramTypes = [];

    /**
     * @var int
     */
    private $defaultFetchMode = FetchMode::MIXED;

    /**
     * @var string Default class to be used by FETCH_CLASS or FETCH_OBJ
     */
    private $defaultFetchClass = '\stdClass';

    /**
     * @var int
     */
    private $defaultFetchColumn = 0;

    /**
     * @var array Parameters to be passed to constructor in FETCH_CLASS
     */
    private $defaultFetchClassCtorArgs = [];

    /**
     * @var int
     */
    private $affectedRows = 0;

    /**
     * @var int|bool
     */
    private $numFields = false;

    /**
     * @param IbaseConnection $connection
     * @param string          $statement
     *
     * @throws \Doctrine\DBAL\Driver\Ibase\IbaseException
     */
    public function __construct(IbaseConnection $connection, $statement)
    {
        $this->connection = $connection;
        $this->statementResource = @ibase_prepare(
            $this->connection->getTransactionResource(),
            $statement
        );
        is_resource($this->statementResource) || $this->checkLastApiCallAndThrowOnError();
    }

    public function __destruct()
    {
        $this->closeCursor();
        if (is_resource($this->statementResource)) {
            @ibase_free_query($this->statementResource);
        }
    }

    /**
     * {@inheritDoc}
     */
    public function setFetchMode($fetchMode, $arg2 = null, $arg3 = null)
    {
        switch ($fetchMode) {
            case FetchMode::CUSTOM_OBJECT:
                $this->defaultFetchMode = $fetchMode;
                $this->defaultFetchClass = is_string($arg2) ? $arg2 : '\stdClass';
                $this->defaultFetchClassCtorArgs = is_array($arg3) ? $arg3 : [];
                break;
            case FetchMode::COLUMN:
                $this->defaultFetchMode = $fetchMode;
                $this->defaultFetchColumn = isset($arg2) ? $arg2 : 0;
                break;
            default:
                $this->defaultFetchMode = $fetchMode;
        }
    }

    /**
     * {@inheritdoc}
     *
     * @throws \Doctrine\DBAL\Driver\Ibase\IbaseException
     */
    public function bindValue($param, $value, $type = null)
    {
        if (!is_numeric($param)) {
            throw new IbaseException(
                'ibase does not support named parameters to queries, use question mark (?) placeholders instead.'
            );
        }

        $this->paramBindings[$param] = $value;
        $this->paramTypes[$param] = $type;
    }

    /**
     * {@inheritdoc}
     *
     * @throws \Doctrine\DBAL\Driver\Ibase\IbaseException
     */
    public function bindParam($column, &$variable, $type = null, $length = null)
    {
        if (!is_numeric($column)) {
            throw new IbaseException("ibase does not support named parameters to queries, use question mark (?) placeholders instead.");
        }

        $this->paramBindings[$column] =& $variable;
        $this->paramTypes[$column] = $type;
    }

    /**
     * {@inheritdoc}
     */
    public function closeCursor()
    {
        if (is_resource($this->resultResource)) {
            @ibase_free_result($this->resultResource);
        }
        $this->resultResource = false;
    }

    /**
     * {@inheritdoc}
     */
    public function columnCount()
    {
        return $this->numFields === false ? 0 : $this->numFields;
    }

    /**
     * {@inheritdoc}
     *
     * @throws \Doctrine\DBAL\Driver\Ibase\IbaseException
     */
    public function execute($params = null)
    {
        if ($params) {
            $hasZeroIndex = array_key_exists(0, $params);
            foreach ($params as $key => $val) {
                $key = ($hasZeroIndex && is_numeric($key)) ? $key + 1 : $key;
                $this->bindValue($key, $val);
            }
        }

        $callArgs = $this->paramBindings;
        array_unshift($callArgs, $this->statementResource);

        $this->resultResource = @call_user_func_array('ibase_execute', $callArgs);
        if ($this->resultResource === false) {
            $this->checkLastApiCallAndThrowOnError();
        }

        if (is_resource($this->resultResource)) {
            $this->affectedRows = ibase_affected_rows($this->connection->getTransactionResource());
            $this->numFields = @ibase_num_fields($this->resultResource);
        } elseif (is_numeric($this->resultResource)) {
            $this->affectedRows = $this->resultResource;
            $this->numFields = false;
            $this->resultResource = false;
        } else {
            $this->affectedRows = 0;
            $this->numFields = false;
            $this->resultResource = false;
        }

        $this->connection->doAutoCommitIfEnabled();

        return true;
    }

    /**
     * {@inheritdoc}
     */
    public function getIterator()
    {
        return new StatementIterator($this);
    }

    /**
     * {@inheritdoc}
     *
     * @throws \Doctrine\DBAL\Driver\Ibase\IbaseException
     */
    public function fetch($fetchMode = null, $cursorOrientation = \PDO::FETCH_ORI_NEXT, $cursorOffset = 0)
    {
        if (!$this->resultResource) {
            return false;
        }

        $fetchMode = $fetchMode ?: $this->defaultFetchMode;

        switch ($fetchMode) {
            case FetchMode::COLUMN:
                return $this->fetchColumn();
            case FetchMode::STANDARD_OBJECT:
                return @ibase_fetch_object($this->resultResource, self::FETCH_FLAG);
            case FetchMode::CUSTOM_OBJECT:
                $className = $this->defaultFetchClass;
                $ctorArgs = $this->defaultFetchClassCtorArgs;

                if (func_num_args() >= 2) {
                    $args = func_get_args();
                    $className = $args[1];
                    $ctorArgs = $args[2] ?? [];
                }

                $result = @ibase_fetch_object($this->resultResource, self::FETCH_FLAG);

                if ($result instanceof \stdClass) {
                    $result = $this->castObject($result, $className, $ctorArgs);
                }

                return $result;
            case FetchMode::ASSOCIATIVE:
                return @ibase_fetch_assoc($this->resultResource, self::FETCH_FLAG);
            case FetchMode::NUMERIC:
                return @ibase_fetch_row($this->resultResource, self::FETCH_FLAG);
            case FetchMode::MIXED:
                $tmpData = ibase_fetch_assoc($this->resultResource, self::FETCH_FLAG);

                return $tmpData === false ? false : array_merge(array_values($tmpData), $tmpData);
            default:
                throw new IbaseException(sprintf("Fetch mode '%s' not supported by this driver", $fetchMode));
        }
    }

    /**
     * {@inheritDoc}
     *
     * @throws \Doctrine\DBAL\Driver\Ibase\IbaseException
     */
    public function fetchAll($fetchMode = null, $fetchArgument = null, $ctorArgs = null)
    {
        $rows = [];

        switch ($fetchMode) {
            case FetchMode::CUSTOM_OBJECT:
                while (($row = $this->fetch(...func_get_args())) !== false) {
                    $rows[] = $row;
                }
                break;
            case FetchMode::COLUMN:
                while (($row = $this->fetchColumn()) !== false) {
                    $rows[] = $row;
                }
                break;
            default:
                while (($row = $this->fetch($fetchMode)) !== false) {
                    $rows[] = $row;
                }
        }

        return $rows;
    }

    /**
     * {@inheritdoc}
     *
     * @throws \Doctrine\DBAL\Driver\Ibase\IbaseException
     */
    public function fetchColumn($columnIndex = 0)
    {
        $row = $this->fetch(FetchMode::NUMERIC);

        if (false === $row) {
            return false;
        }

        return $row[$columnIndex] ?? null;
    }

    /**
     * {@inheritDoc}
     */
    public function rowCount()
    {
        return $this->affectedRows;
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

    /**
     * Casts a stdClass object to the given class name mapping its' properties.
     *
     * @param \stdClass     $sourceObject     Object to cast from.
     * @param string|object $destinationClass Name of the class or class instance to cast to.
     * @param array         $ctorArgs         Arguments to use for constructing the destination class instance.
     *
     * @return object
     *
     * @throws \Doctrine\DBAL\Driver\Ibase\IbaseException
     */
    private function castObject(\stdClass $sourceObject, $destinationClass, array $ctorArgs = [])
    {
        if (!is_string($destinationClass)) {
            if (!is_object($destinationClass)) {
                throw new IbaseException(sprintf(
                    'Destination class has to be of type string or object, %s given.', gettype($destinationClass)
                ));
            }
        } else {
            try {
                $destinationClass = new \ReflectionClass($destinationClass);
            } catch (\ReflectionException $e) {
                throw new IbaseException(sprintf(
                    "Unable to cast to object of type '%s'. %s", gettype($destinationClass), $e->getMessage()
                ));
            }

            $destinationClass = $destinationClass->newInstanceArgs($ctorArgs);
        }

        $sourceReflection = new \ReflectionObject($sourceObject);
        $destinationClassReflection = new \ReflectionObject($destinationClass);
        /** @var \ReflectionProperty[] $destinationProperties */
        $destinationProperties = array_change_key_case($destinationClassReflection->getProperties(), \CASE_UPPER);

        foreach ($sourceReflection->getProperties() as $sourceProperty) {
            $sourceProperty->setAccessible(true);

            $name = $sourceProperty->getName();
            $value = $sourceProperty->getValue($sourceObject);

            // Try to find a case-matching property.
            if ($destinationClassReflection->hasProperty($name)) {
                $destinationProperty = $destinationClassReflection->getProperty($name);

                $destinationProperty->setAccessible(true);
                $destinationProperty->setValue($destinationClass, $value);

                continue;
            }

            $name = strtoupper($name);

            // Try to find a property without matching case.
            // Fallback for the driver returning either all uppercase or all lowercase column names.
            if (isset($destinationProperties[$name])) {
                $destinationProperty = $destinationProperties[$name];

                $destinationProperty->setAccessible(true);
                $destinationProperty->setValue($destinationClass, $value);

                continue;
            }

            $destinationClass->$name = $value;
        }

        return $destinationClass;
    }
}
