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

namespace Doctrine\DBAL\Driver;

use Doctrine\DBAL\Connection;
use Doctrine\DBAL\Driver;
use Doctrine\DBAL\Driver\DriverException as DriverDriverException;
use Doctrine\DBAL\Exception\ConnectionException;
use Doctrine\DBAL\Exception\DriverException;
use Doctrine\DBAL\Exception\ForeignKeyConstraintViolationException;
use Doctrine\DBAL\Exception\InvalidFieldNameException;
use Doctrine\DBAL\Exception\NonUniqueFieldNameException;
use Doctrine\DBAL\Exception\SyntaxErrorException;
use Doctrine\DBAL\Exception\TableExistsException;
use Doctrine\DBAL\Exception\TableNotFoundException;
use Doctrine\DBAL\Exception\UniqueConstraintViolationException;
use Doctrine\DBAL\Platforms\IbasePlatform;
use Doctrine\DBAL\Schema\IbaseSchemaManager;
use function preg_match;

/**
 * Abstract Interbase/Firebird driver.
 *
 * @author Andreas Prucha, Helicon Software Development <prucha@helicon.co.at>
 * @author Douglas Hammond <wizhippo@gmail.com>
 */
abstract class AbstractIbaseDriver implements Driver, ExceptionConverterDriver
{
    /**
     * {@inheritDoc}
     *
     * @param \Doctrine\DBAL\Driver\DriverException $exception
     */
    public function convertException($message, DriverDriverException $exception)
    {
        $message = 'Error ' . $exception->getErrorCode() . ': ' . $message;
        switch ($exception->getErrorCode()) {
            case -104:
                return new SyntaxErrorException($message, $exception);
            case -204:
                if (preg_match('/.*(dynamic sql error).*(table unknown).*/i', $message)) {
                    return new TableNotFoundException($message, $exception);
                }
                if (preg_match('/.*(dynamic sql error).*(ambiguous field name).*/i', $message)) {
                    return new NonUniqueFieldNameException($message, $exception);
                }
                break;
            case -206:
                if (preg_match('/.*(dynamic sql error).*(table unknown).*/i', $message)) {
                    return new InvalidFieldNameException($message, $exception);
                }
                if (preg_match('/.*(dynamic sql error).*(column unknown).*/i', $message)) {
                    return new InvalidFieldNameException($message, $exception);
                }
                break;
            case -803:
                return new UniqueConstraintViolationException($message, $exception);
            case -530:
                return new ForeignKeyConstraintViolationException($message, $exception);
            case -607:
                if (preg_match('/.*(unsuccessful metadata update Table).*(already exists).*/i', $message)) {
                    return new TableExistsException($message, $exception);
                }
                break;
            case -902:
                return new ConnectionException($message, $exception);
            default:
                return new DriverException($message, $exception);
        }
    }

    /**
     * {@inheritDoc}
     */
    public function getDatabasePlatform()
    {
        return new IbasePlatform();
    }

    /**
     * {@inheritDoc}
     */
    public function getSchemaManager(Connection $conn)
    {
        return new IbaseSchemaManager($conn);
    }

    /**
     * {@inheritDoc}
     */
    public function getDatabase(Connection $conn)
    {
        $params = $conn->getParams();

        return $params['dbname'];
    }
}
