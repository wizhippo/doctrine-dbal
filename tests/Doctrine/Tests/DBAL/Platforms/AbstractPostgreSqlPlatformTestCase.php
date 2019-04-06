<?php

namespace Doctrine\Tests\DBAL\Platforms;

use Doctrine\DBAL\Schema\Column;
use Doctrine\DBAL\Schema\ColumnDiff;
use Doctrine\DBAL\Schema\Comparator;
use Doctrine\DBAL\Schema\ForeignKeyConstraint;
use Doctrine\DBAL\Schema\Sequence;
use Doctrine\DBAL\Schema\Table;
use Doctrine\DBAL\Schema\TableDiff;
use Doctrine\DBAL\TransactionIsolationLevel;
use Doctrine\DBAL\Types\Type;
use function sprintf;

abstract class AbstractPostgreSqlPlatformTestCase extends AbstractPlatformTestCase
{
    public function getGenerateTableSql()
    {
        return 'CREATE TABLE test (id SERIAL NOT NULL, test VARCHAR(255) DEFAULT NULL, PRIMARY KEY(id))';
    }

    public function getGenerateTableWithMultiColumnUniqueIndexSql()
    {
        return [
            'CREATE TABLE test (foo VARCHAR(255) DEFAULT NULL, bar VARCHAR(255) DEFAULT NULL)',
            'CREATE UNIQUE INDEX UNIQ_D87F7E0C8C73652176FF8CAA ON test (foo, bar)',
        ];
    }

    public function getGenerateAlterTableSql()
    {
        return [
            'ALTER TABLE mytable ADD quota INT DEFAULT NULL',
            'ALTER TABLE mytable DROP foo',
            'ALTER TABLE mytable ALTER bar TYPE VARCHAR(255)',
            "ALTER TABLE mytable ALTER bar SET DEFAULT 'def'",
            'ALTER TABLE mytable ALTER bar SET NOT NULL',
            'ALTER TABLE mytable ALTER bloo TYPE BOOLEAN',
            "ALTER TABLE mytable ALTER bloo SET DEFAULT 'false'",
            'ALTER TABLE mytable ALTER bloo SET NOT NULL',
            'ALTER TABLE mytable RENAME TO userlist',
        ];
    }

    public function getGenerateIndexSql()
    {
        return 'CREATE INDEX my_idx ON mytable (user_name, last_login)';
    }

    public function getGenerateForeignKeySql()
    {
        return 'ALTER TABLE test ADD FOREIGN KEY (fk_name_id) REFERENCES other_table (id) NOT DEFERRABLE INITIALLY IMMEDIATE';
    }

    public function testGeneratesForeignKeySqlForNonStandardOptions()
    {
        $foreignKey = new ForeignKeyConstraint(
            ['foreign_id'],
            'my_table',
            ['id'],
            'my_fk',
            ['onDelete' => 'CASCADE']
        );
        self::assertEquals(
            'CONSTRAINT my_fk FOREIGN KEY (foreign_id) REFERENCES my_table (id) ON DELETE CASCADE NOT DEFERRABLE INITIALLY IMMEDIATE',
            $this->platform->getForeignKeyDeclarationSQL($foreignKey)
        );

        $foreignKey = new ForeignKeyConstraint(
            ['foreign_id'],
            'my_table',
            ['id'],
            'my_fk',
            ['match' => 'full']
        );
        self::assertEquals(
            'CONSTRAINT my_fk FOREIGN KEY (foreign_id) REFERENCES my_table (id) MATCH full NOT DEFERRABLE INITIALLY IMMEDIATE',
            $this->platform->getForeignKeyDeclarationSQL($foreignKey)
        );

        $foreignKey = new ForeignKeyConstraint(
            ['foreign_id'],
            'my_table',
            ['id'],
            'my_fk',
            ['deferrable' => true]
        );
        self::assertEquals(
            'CONSTRAINT my_fk FOREIGN KEY (foreign_id) REFERENCES my_table (id) DEFERRABLE INITIALLY IMMEDIATE',
            $this->platform->getForeignKeyDeclarationSQL($foreignKey)
        );

        $foreignKey = new ForeignKeyConstraint(
            ['foreign_id'],
            'my_table',
            ['id'],
            'my_fk',
            ['deferred' => true]
        );
        self::assertEquals(
            'CONSTRAINT my_fk FOREIGN KEY (foreign_id) REFERENCES my_table (id) NOT DEFERRABLE INITIALLY DEFERRED',
            $this->platform->getForeignKeyDeclarationSQL($foreignKey)
        );

        $foreignKey = new ForeignKeyConstraint(
            ['foreign_id'],
            'my_table',
            ['id'],
            'my_fk',
            ['feferred' => true]
        );
        self::assertEquals(
            'CONSTRAINT my_fk FOREIGN KEY (foreign_id) REFERENCES my_table (id) NOT DEFERRABLE INITIALLY DEFERRED',
            $this->platform->getForeignKeyDeclarationSQL($foreignKey)
        );

        $foreignKey = new ForeignKeyConstraint(
            ['foreign_id'],
            'my_table',
            ['id'],
            'my_fk',
            ['deferrable' => true, 'deferred' => true, 'match' => 'full']
        );
        self::assertEquals(
            'CONSTRAINT my_fk FOREIGN KEY (foreign_id) REFERENCES my_table (id) MATCH full DEFERRABLE INITIALLY DEFERRED',
            $this->platform->getForeignKeyDeclarationSQL($foreignKey)
        );
    }

    public function testGeneratesSqlSnippets()
    {
        self::assertEquals('SIMILAR TO', $this->platform->getRegexpExpression(), 'Regular expression operator is not correct');
        self::assertEquals('"', $this->platform->getIdentifierQuoteCharacter(), 'Identifier quote character is not correct');
        self::assertEquals('column1 || column2 || column3', $this->platform->getConcatExpression('column1', 'column2', 'column3'), 'Concatenation expression is not correct');
        self::assertEquals('SUBSTRING(column FROM 5)', $this->platform->getSubstringExpression('column', 5), 'Substring expression without length is not correct');
        self::assertEquals('SUBSTRING(column FROM 1 FOR 5)', $this->platform->getSubstringExpression('column', 1, 5), 'Substring expression with length is not correct');
    }

    public function testGeneratesTransactionCommands()
    {
        self::assertEquals(
            'SET SESSION CHARACTERISTICS AS TRANSACTION ISOLATION LEVEL READ UNCOMMITTED',
            $this->platform->getSetTransactionIsolationSQL(TransactionIsolationLevel::READ_UNCOMMITTED)
        );
        self::assertEquals(
            'SET SESSION CHARACTERISTICS AS TRANSACTION ISOLATION LEVEL READ COMMITTED',
            $this->platform->getSetTransactionIsolationSQL(TransactionIsolationLevel::READ_COMMITTED)
        );
        self::assertEquals(
            'SET SESSION CHARACTERISTICS AS TRANSACTION ISOLATION LEVEL REPEATABLE READ',
            $this->platform->getSetTransactionIsolationSQL(TransactionIsolationLevel::REPEATABLE_READ)
        );
        self::assertEquals(
            'SET SESSION CHARACTERISTICS AS TRANSACTION ISOLATION LEVEL SERIALIZABLE',
            $this->platform->getSetTransactionIsolationSQL(TransactionIsolationLevel::SERIALIZABLE)
        );
    }

    public function testGeneratesDDLSnippets()
    {
        self::assertEquals('CREATE DATABASE foobar', $this->platform->getCreateDatabaseSQL('foobar'));
        self::assertEquals('DROP DATABASE foobar', $this->platform->getDropDatabaseSQL('foobar'));
        self::assertEquals('DROP TABLE foobar', $this->platform->getDropTableSQL('foobar'));
    }

    public function testGenerateTableWithAutoincrement()
    {
        $table  = new Table('autoinc_table');
        $column = $table->addColumn('id', 'integer');
        $column->setAutoincrement(true);

        self::assertEquals(['CREATE TABLE autoinc_table (id SERIAL NOT NULL)'], $this->platform->getCreateTableSQL($table));
    }

    /**
     * @return string[][]
     */
    public static function serialTypes() : array
    {
        return [
            ['integer', 'SERIAL'],
            ['bigint', 'BIGSERIAL'],
        ];
    }

    /**
     * @dataProvider serialTypes
     * @group 2906
     */
    public function testGenerateTableWithAutoincrementDoesNotSetDefault(string $type, string $definition) : void
    {
        $table  = new Table('autoinc_table_notnull');
        $column = $table->addColumn('id', $type);
        $column->setAutoIncrement(true);
        $column->setNotNull(false);

        $sql = $this->platform->getCreateTableSQL($table);

        self::assertEquals([sprintf('CREATE TABLE autoinc_table_notnull (id %s)', $definition)], $sql);
    }

    /**
     * @dataProvider serialTypes
     * @group 2906
     */
    public function testCreateTableWithAutoincrementAndNotNullAddsConstraint(string $type, string $definition) : void
    {
        $table  = new Table('autoinc_table_notnull_enabled');
        $column = $table->addColumn('id', $type);
        $column->setAutoIncrement(true);
        $column->setNotNull(true);

        $sql = $this->platform->getCreateTableSQL($table);

        self::assertEquals([sprintf('CREATE TABLE autoinc_table_notnull_enabled (id %s NOT NULL)', $definition)], $sql);
    }

    /**
     * @dataProvider serialTypes
     * @group 2906
     */
    public function testGetDefaultValueDeclarationSQLIgnoresTheDefaultKeyWhenTheFieldIsSerial(string $type) : void
    {
        $sql = $this->platform->getDefaultValueDeclarationSQL(
            [
                'autoincrement' => true,
                'type'          => Type::getType($type),
                'default'       => 1,
            ]
        );

        self::assertSame('', $sql);
    }

    public function testGeneratesTypeDeclarationForIntegers()
    {
        self::assertEquals(
            'INT',
            $this->platform->getIntegerTypeDeclarationSQL([])
        );
        self::assertEquals(
            'SERIAL',
            $this->platform->getIntegerTypeDeclarationSQL(['autoincrement' => true])
        );
        self::assertEquals(
            'SERIAL',
            $this->platform->getIntegerTypeDeclarationSQL(
                ['autoincrement' => true, 'primary' => true]
            )
        );
    }

    public function testGeneratesTypeDeclarationForStrings()
    {
        self::assertEquals(
            'CHAR(10)',
            $this->platform->getVarcharTypeDeclarationSQL(
                ['length' => 10, 'fixed' => true]
            )
        );
        self::assertEquals(
            'VARCHAR(50)',
            $this->platform->getVarcharTypeDeclarationSQL(['length' => 50]),
            'Variable string declaration is not correct'
        );
        self::assertEquals(
            'VARCHAR(255)',
            $this->platform->getVarcharTypeDeclarationSQL([]),
            'Long string declaration is not correct'
        );
    }

    public function getGenerateUniqueIndexSql()
    {
        return 'CREATE UNIQUE INDEX index_name ON test (test, test2)';
    }

    public function testGeneratesSequenceSqlCommands()
    {
        $sequence = new Sequence('myseq', 20, 1);
        self::assertEquals(
            'CREATE SEQUENCE myseq INCREMENT BY 20 MINVALUE 1 START 1',
            $this->platform->getCreateSequenceSQL($sequence)
        );
        self::assertEquals(
            'DROP SEQUENCE myseq CASCADE',
            $this->platform->getDropSequenceSQL('myseq')
        );
        self::assertEquals(
            "SELECT NEXTVAL('myseq')",
            $this->platform->getSequenceNextValSQL('myseq')
        );
    }

    public function testDoesNotPreferIdentityColumns()
    {
        self::assertFalse($this->platform->prefersIdentityColumns());
    }

    public function testPrefersSequences()
    {
        self::assertTrue($this->platform->prefersSequences());
    }

    public function testSupportsIdentityColumns()
    {
        self::assertTrue($this->platform->supportsIdentityColumns());
    }

    public function testSupportsSavePoints()
    {
        self::assertTrue($this->platform->supportsSavepoints());
    }

    public function testSupportsSequences()
    {
        self::assertTrue($this->platform->supportsSequences());
    }

    /**
     * {@inheritdoc}
     */
    protected function supportsCommentOnStatement()
    {
        return true;
    }

    public function testModifyLimitQuery()
    {
        $sql = $this->platform->modifyLimitQuery('SELECT * FROM user', 10, 0);
        self::assertEquals('SELECT * FROM user LIMIT 10', $sql);
    }

    public function testModifyLimitQueryWithEmptyOffset()
    {
        $sql = $this->platform->modifyLimitQuery('SELECT * FROM user', 10);
        self::assertEquals('SELECT * FROM user LIMIT 10', $sql);
    }

    public function getCreateTableColumnCommentsSQL()
    {
        return [
            'CREATE TABLE test (id INT NOT NULL, PRIMARY KEY(id))',
            "COMMENT ON COLUMN test.id IS 'This is a comment'",
        ];
    }

    public function getAlterTableColumnCommentsSQL()
    {
        return [
            'ALTER TABLE mytable ADD quota INT NOT NULL',
            "COMMENT ON COLUMN mytable.quota IS 'A comment'",
            'COMMENT ON COLUMN mytable.foo IS NULL',
            "COMMENT ON COLUMN mytable.baz IS 'B comment'",
        ];
    }

    public function getCreateTableColumnTypeCommentsSQL()
    {
        return [
            'CREATE TABLE test (id INT NOT NULL, data TEXT NOT NULL, PRIMARY KEY(id))',
            "COMMENT ON COLUMN test.data IS '(DC2Type:array)'",
        ];
    }

    protected function getQuotedColumnInPrimaryKeySQL()
    {
        return ['CREATE TABLE "quoted" ("create" VARCHAR(255) NOT NULL, PRIMARY KEY("create"))'];
    }

    protected function getQuotedColumnInIndexSQL()
    {
        return [
            'CREATE TABLE "quoted" ("create" VARCHAR(255) NOT NULL)',
            'CREATE INDEX IDX_22660D028FD6E0FB ON "quoted" ("create")',
        ];
    }

    protected function getQuotedNameInIndexSQL()
    {
        return [
            'CREATE TABLE test (column1 VARCHAR(255) NOT NULL)',
            'CREATE INDEX "key" ON test (column1)',
        ];
    }

    protected function getQuotedColumnInForeignKeySQL()
    {
        return [
            'CREATE TABLE "quoted" ("create" VARCHAR(255) NOT NULL, foo VARCHAR(255) NOT NULL, "bar" VARCHAR(255) NOT NULL)',
            'ALTER TABLE "quoted" ADD CONSTRAINT FK_WITH_RESERVED_KEYWORD FOREIGN KEY ("create", foo, "bar") REFERENCES "foreign" ("create", bar, "foo-bar") NOT DEFERRABLE INITIALLY IMMEDIATE',
            'ALTER TABLE "quoted" ADD CONSTRAINT FK_WITH_NON_RESERVED_KEYWORD FOREIGN KEY ("create", foo, "bar") REFERENCES foo ("create", bar, "foo-bar") NOT DEFERRABLE INITIALLY IMMEDIATE',
            'ALTER TABLE "quoted" ADD CONSTRAINT FK_WITH_INTENDED_QUOTATION FOREIGN KEY ("create", foo, "bar") REFERENCES "foo-bar" ("create", bar, "foo-bar") NOT DEFERRABLE INITIALLY IMMEDIATE',
        ];
    }

    /**
     * @param string $databaseValue
     * @param string $preparedStatementValue
     * @param int    $integerValue
     * @param bool   $booleanValue
     *
     * @group DBAL-457
     * @dataProvider pgBooleanProvider
     */
    public function testConvertBooleanAsLiteralStrings(
        $databaseValue,
        $preparedStatementValue,
        $integerValue,
        $booleanValue
    ) {
        $platform = $this->createPlatform();

        self::assertEquals($preparedStatementValue, $platform->convertBooleans($databaseValue));
    }

    /**
     * @group DBAL-457
     */
    public function testConvertBooleanAsLiteralIntegers()
    {
        $platform = $this->createPlatform();
        $platform->setUseBooleanTrueFalseStrings(false);

        self::assertEquals(1, $platform->convertBooleans(true));
        self::assertEquals(1, $platform->convertBooleans('1'));

        self::assertEquals(0, $platform->convertBooleans(false));
        self::assertEquals(0, $platform->convertBooleans('0'));
    }

    /**
     * @param string $databaseValue
     * @param string $preparedStatementValue
     * @param int    $integerValue
     * @param bool   $booleanValue
     *
     * @group DBAL-630
     * @dataProvider pgBooleanProvider
     */
    public function testConvertBooleanAsDatabaseValueStrings(
        $databaseValue,
        $preparedStatementValue,
        $integerValue,
        $booleanValue
    ) {
        $platform = $this->createPlatform();

        self::assertSame($integerValue, $platform->convertBooleansToDatabaseValue($booleanValue));
    }

    /**
     * @group DBAL-630
     */
    public function testConvertBooleanAsDatabaseValueIntegers()
    {
        $platform = $this->createPlatform();
        $platform->setUseBooleanTrueFalseStrings(false);

        self::assertSame(1, $platform->convertBooleansToDatabaseValue(true));
        self::assertSame(0, $platform->convertBooleansToDatabaseValue(false));
    }

    /**
     * @param string $databaseValue
     * @param string $prepareStatementValue
     * @param int    $integerValue
     * @param bool   $booleanValue
     *
     * @dataProvider pgBooleanProvider
     */
    public function testConvertFromBoolean($databaseValue, $prepareStatementValue, $integerValue, $booleanValue)
    {
        $platform = $this->createPlatform();

        self::assertSame($booleanValue, $platform->convertFromBoolean($databaseValue));
    }

    /**
     * @expectedException        UnexpectedValueException
     * @expectedExceptionMessage Unrecognized boolean literal 'my-bool'
     */
    public function testThrowsExceptionWithInvalidBooleanLiteral()
    {
        $platform = $this->createPlatform()->convertBooleansToDatabaseValue('my-bool');
    }

    public function testGetCreateSchemaSQL()
    {
        $schemaName = 'schema';
        $sql        = $this->platform->getCreateSchemaSQL($schemaName);
        self::assertEquals('CREATE SCHEMA ' . $schemaName, $sql);
    }

    public function testAlterDecimalPrecisionScale()
    {
        $table = new Table('mytable');
        $table->addColumn('dfoo1', 'decimal');
        $table->addColumn('dfoo2', 'decimal', ['precision' => 10, 'scale' => 6]);
        $table->addColumn('dfoo3', 'decimal', ['precision' => 10, 'scale' => 6]);
        $table->addColumn('dfoo4', 'decimal', ['precision' => 10, 'scale' => 6]);

        $tableDiff            = new TableDiff('mytable');
        $tableDiff->fromTable = $table;

        $tableDiff->changedColumns['dloo1'] = new ColumnDiff(
            'dloo1',
            new Column(
                'dloo1',
                Type::getType('decimal'),
                ['precision' => 16, 'scale' => 6]
            ),
            ['precision']
        );
        $tableDiff->changedColumns['dloo2'] = new ColumnDiff(
            'dloo2',
            new Column(
                'dloo2',
                Type::getType('decimal'),
                ['precision' => 10, 'scale' => 4]
            ),
            ['scale']
        );
        $tableDiff->changedColumns['dloo3'] = new ColumnDiff(
            'dloo3',
            new Column(
                'dloo3',
                Type::getType('decimal'),
                ['precision' => 10, 'scale' => 6]
            ),
            []
        );
        $tableDiff->changedColumns['dloo4'] = new ColumnDiff(
            'dloo4',
            new Column(
                'dloo4',
                Type::getType('decimal'),
                ['precision' => 16, 'scale' => 8]
            ),
            ['precision', 'scale']
        );

        $sql = $this->platform->getAlterTableSQL($tableDiff);

        $expectedSql = [
            'ALTER TABLE mytable ALTER dloo1 TYPE NUMERIC(16, 6)',
            'ALTER TABLE mytable ALTER dloo2 TYPE NUMERIC(10, 4)',
            'ALTER TABLE mytable ALTER dloo4 TYPE NUMERIC(16, 8)',
        ];

        self::assertEquals($expectedSql, $sql);
    }

    /**
     * @group DBAL-365
     */
    public function testDroppingConstraintsBeforeColumns()
    {
        $newTable = new Table('mytable');
        $newTable->addColumn('id', 'integer');
        $newTable->setPrimaryKey(['id']);

        $oldTable = clone $newTable;
        $oldTable->addColumn('parent_id', 'integer');
        $oldTable->addUnnamedForeignKeyConstraint('mytable', ['parent_id'], ['id']);

        $comparator = new Comparator();
        $tableDiff  = $comparator->diffTable($oldTable, $newTable);

        $sql = $this->platform->getAlterTableSQL($tableDiff);

        $expectedSql = [
            'ALTER TABLE mytable DROP CONSTRAINT FK_6B2BD609727ACA70',
            'DROP INDEX IDX_6B2BD609727ACA70',
            'ALTER TABLE mytable DROP parent_id',
        ];

        self::assertEquals($expectedSql, $sql);
    }

    /**
     * @group DBAL-563
     */
    public function testUsesSequenceEmulatedIdentityColumns()
    {
        self::assertTrue($this->platform->usesSequenceEmulatedIdentityColumns());
    }

    /**
     * @group DBAL-563
     */
    public function testReturnsIdentitySequenceName()
    {
        self::assertSame('mytable_mycolumn_seq', $this->platform->getIdentitySequenceName('mytable', 'mycolumn'));
    }

    /**
     * @dataProvider dataCreateSequenceWithCache
     * @group DBAL-139
     */
    public function testCreateSequenceWithCache($cacheSize, $expectedSql)
    {
        $sequence = new Sequence('foo', 1, 1, $cacheSize);
        self::assertContains($expectedSql, $this->platform->getCreateSequenceSQL($sequence));
    }

    public function dataCreateSequenceWithCache()
    {
        return [
            [3, 'CACHE 3'],
        ];
    }

    protected function getBinaryDefaultLength()
    {
        return 0;
    }

    protected function getBinaryMaxLength()
    {
        return 0;
    }

    public function testReturnsBinaryTypeDeclarationSQL()
    {
        self::assertSame('BYTEA', $this->platform->getBinaryTypeDeclarationSQL([]));
        self::assertSame('BYTEA', $this->platform->getBinaryTypeDeclarationSQL(['length' => 0]));
        self::assertSame('BYTEA', $this->platform->getBinaryTypeDeclarationSQL(['length' => 9999999]));

        self::assertSame('BYTEA', $this->platform->getBinaryTypeDeclarationSQL(['fixed' => true]));
        self::assertSame('BYTEA', $this->platform->getBinaryTypeDeclarationSQL(['fixed' => true, 'length' => 0]));
        self::assertSame('BYTEA', $this->platform->getBinaryTypeDeclarationSQL(['fixed' => true, 'length' => 9999999]));
    }

    public function testDoesNotPropagateUnnecessaryTableAlterationOnBinaryType()
    {
        $table1 = new Table('mytable');
        $table1->addColumn('column_varbinary', 'binary');
        $table1->addColumn('column_binary', 'binary', ['fixed' => true]);
        $table1->addColumn('column_blob', 'blob');

        $table2 = new Table('mytable');
        $table2->addColumn('column_varbinary', 'binary', ['fixed' => true]);
        $table2->addColumn('column_binary', 'binary');
        $table2->addColumn('column_blob', 'binary');

        $comparator = new Comparator();

        // VARBINARY -> BINARY
        // BINARY    -> VARBINARY
        // BLOB      -> VARBINARY
        self::assertEmpty($this->platform->getAlterTableSQL($comparator->diffTable($table1, $table2)));

        $table2 = new Table('mytable');
        $table2->addColumn('column_varbinary', 'binary', ['length' => 42]);
        $table2->addColumn('column_binary', 'blob');
        $table2->addColumn('column_blob', 'binary', ['length' => 11, 'fixed' => true]);

        // VARBINARY -> VARBINARY with changed length
        // BINARY    -> BLOB
        // BLOB      -> BINARY
        self::assertEmpty($this->platform->getAlterTableSQL($comparator->diffTable($table1, $table2)));

        $table2 = new Table('mytable');
        $table2->addColumn('column_varbinary', 'blob');
        $table2->addColumn('column_binary', 'binary', ['length' => 42, 'fixed' => true]);
        $table2->addColumn('column_blob', 'blob');

        // VARBINARY -> BLOB
        // BINARY    -> BINARY with changed length
        // BLOB      -> BLOB
        self::assertEmpty($this->platform->getAlterTableSQL($comparator->diffTable($table1, $table2)));
    }

    /**
     * @group DBAL-234
     */
    protected function getAlterTableRenameIndexSQL()
    {
        return ['ALTER INDEX idx_foo RENAME TO idx_bar'];
    }

    /**
     * @group DBAL-234
     */
    protected function getQuotedAlterTableRenameIndexSQL()
    {
        return [
            'ALTER INDEX "create" RENAME TO "select"',
            'ALTER INDEX "foo" RENAME TO "bar"',
        ];
    }

    /**
     * PostgreSQL boolean strings provider
     *
     * @return mixed[]
     */
    public function pgBooleanProvider()
    {
        return [
            // Database value, prepared statement value, boolean integer value, boolean value.
            [true, 'true', 1, true],
            ['t', 'true', 1, true],
            ['true', 'true', 1, true],
            ['y', 'true', 1, true],
            ['yes', 'true', 1, true],
            ['on', 'true', 1, true],
            ['1', 'true', 1, true],

            [false, 'false', 0, false],
            ['f', 'false', 0, false],
            ['false', 'false', 0, false],
            [ 'n', 'false', 0, false],
            ['no', 'false', 0, false],
            ['off', 'false', 0, false],
            ['0', 'false', 0, false],

            [null, 'NULL', null, null],
        ];
    }

    /**
     * {@inheritdoc}
     */
    protected function getQuotedAlterTableRenameColumnSQL()
    {
        return [
            'ALTER TABLE mytable RENAME COLUMN unquoted1 TO unquoted',
            'ALTER TABLE mytable RENAME COLUMN unquoted2 TO "where"',
            'ALTER TABLE mytable RENAME COLUMN unquoted3 TO "foo"',
            'ALTER TABLE mytable RENAME COLUMN "create" TO reserved_keyword',
            'ALTER TABLE mytable RENAME COLUMN "table" TO "from"',
            'ALTER TABLE mytable RENAME COLUMN "select" TO "bar"',
            'ALTER TABLE mytable RENAME COLUMN quoted1 TO quoted',
            'ALTER TABLE mytable RENAME COLUMN quoted2 TO "and"',
            'ALTER TABLE mytable RENAME COLUMN quoted3 TO "baz"',
        ];
    }

    /**
     * {@inheritdoc}
     */
    protected function getQuotedAlterTableChangeColumnLengthSQL()
    {
        return [
            'ALTER TABLE mytable ALTER unquoted1 TYPE VARCHAR(255)',
            'ALTER TABLE mytable ALTER unquoted2 TYPE VARCHAR(255)',
            'ALTER TABLE mytable ALTER unquoted3 TYPE VARCHAR(255)',
            'ALTER TABLE mytable ALTER "create" TYPE VARCHAR(255)',
            'ALTER TABLE mytable ALTER "table" TYPE VARCHAR(255)',
            'ALTER TABLE mytable ALTER "select" TYPE VARCHAR(255)',
        ];
    }

    /**
     * @group DBAL-807
     */
    protected function getAlterTableRenameIndexInSchemaSQL()
    {
        return ['ALTER INDEX myschema.idx_foo RENAME TO idx_bar'];
    }

    /**
     * @group DBAL-807
     */
    protected function getQuotedAlterTableRenameIndexInSchemaSQL()
    {
        return [
            'ALTER INDEX "schema"."create" RENAME TO "select"',
            'ALTER INDEX "schema"."foo" RENAME TO "bar"',
        ];
    }

    protected function getQuotesDropForeignKeySQL()
    {
        return 'ALTER TABLE "table" DROP CONSTRAINT "select"';
    }

    public function testGetNullCommentOnColumnSQL()
    {
        self::assertEquals(
            'COMMENT ON COLUMN mytable.id IS NULL',
            $this->platform->getCommentOnColumnSQL('mytable', 'id', null)
        );
    }

    /**
     * @group DBAL-423
     */
    public function testReturnsGuidTypeDeclarationSQL()
    {
        self::assertSame('UUID', $this->platform->getGuidTypeDeclarationSQL([]));
    }

    /**
     * {@inheritdoc}
     */
    public function getAlterTableRenameColumnSQL()
    {
        return ['ALTER TABLE foo RENAME COLUMN bar TO baz'];
    }

    /**
     * {@inheritdoc}
     */
    protected function getQuotesTableIdentifiersInAlterTableSQL()
    {
        return [
            'ALTER TABLE "foo" DROP CONSTRAINT fk1',
            'ALTER TABLE "foo" DROP CONSTRAINT fk2',
            'ALTER TABLE "foo" ADD bloo INT NOT NULL',
            'ALTER TABLE "foo" DROP baz',
            'ALTER TABLE "foo" ALTER bar DROP NOT NULL',
            'ALTER TABLE "foo" RENAME COLUMN id TO war',
            'ALTER TABLE "foo" RENAME TO "table"',
            'ALTER TABLE "table" ADD CONSTRAINT fk_add FOREIGN KEY (fk3) REFERENCES fk_table (id) NOT DEFERRABLE ' .
            'INITIALLY IMMEDIATE',
            'ALTER TABLE "table" ADD CONSTRAINT fk2 FOREIGN KEY (fk2) REFERENCES fk_table2 (id) NOT DEFERRABLE ' .
            'INITIALLY IMMEDIATE',
        ];
    }

    /**
     * {@inheritdoc}
     */
    protected function getCommentOnColumnSQL()
    {
        return [
            'COMMENT ON COLUMN foo.bar IS \'comment\'',
            'COMMENT ON COLUMN "Foo"."BAR" IS \'comment\'',
            'COMMENT ON COLUMN "select"."from" IS \'comment\'',
        ];
    }

    /**
     * @group DBAL-1004
     */
    public function testAltersTableColumnCommentWithExplicitlyQuotedIdentifiers()
    {
        $table1 = new Table('"foo"', [new Column('"bar"', Type::getType('integer'))]);
        $table2 = new Table('"foo"', [new Column('"bar"', Type::getType('integer'), ['comment' => 'baz'])]);

        $comparator = new Comparator();

        $tableDiff = $comparator->diffTable($table1, $table2);

        self::assertInstanceOf(TableDiff::class, $tableDiff);
        self::assertSame(
            ['COMMENT ON COLUMN "foo"."bar" IS \'baz\''],
            $this->platform->getAlterTableSQL($tableDiff)
        );
    }

    /**
     * @group 3158
     */
    public function testAltersTableColumnCommentIfRequiredByType()
    {
        $table1 = new Table('"foo"', [new Column('"bar"', Type::getType('datetime'))]);
        $table2 = new Table('"foo"', [new Column('"bar"', Type::getType('datetime_immutable'))]);

        $comparator = new Comparator();

        $tableDiff = $comparator->diffTable($table1, $table2);

        $this->assertInstanceOf('Doctrine\DBAL\Schema\TableDiff', $tableDiff);
        $this->assertSame(
            [
                'ALTER TABLE "foo" ALTER "bar" TYPE TIMESTAMP(0) WITHOUT TIME ZONE',
                'ALTER TABLE "foo" ALTER "bar" DROP DEFAULT',
                'COMMENT ON COLUMN "foo"."bar" IS \'(DC2Type:datetime_immutable)\'',
            ],
            $this->platform->getAlterTableSQL($tableDiff)
        );
    }

    /**
     * {@inheritdoc}
     */
    protected function getQuotesReservedKeywordInUniqueConstraintDeclarationSQL()
    {
        return 'CONSTRAINT "select" UNIQUE (foo)';
    }

    /**
     * {@inheritdoc}
     */
    protected function getQuotesReservedKeywordInIndexDeclarationSQL()
    {
        return 'INDEX "select" (foo)';
    }

    /**
     * {@inheritdoc}
     */
    protected function getQuotesReservedKeywordInTruncateTableSQL()
    {
        return 'TRUNCATE "select"';
    }

    /**
     * {@inheritdoc}
     */
    protected function getAlterStringToFixedStringSQL()
    {
        return ['ALTER TABLE mytable ALTER name TYPE CHAR(2)'];
    }

    /**
     * @group 3158
     */
    public function testAltersTableColumnCommentIfRequiredByType()
    {
        $table1 = new Table('"foo"', [new Column('"bar"', Type::getType('datetime'))]);
        $table2 = new Table('"foo"', [new Column('"bar"', Type::getType('datetime_immutable'))]);

        $comparator = new Comparator();

        $tableDiff = $comparator->diffTable($table1, $table2);

        $this->assertInstanceOf('Doctrine\DBAL\Schema\TableDiff', $tableDiff);
        $this->assertSame(
            [
                'ALTER TABLE "foo" ALTER "bar" TYPE TIMESTAMP(0) WITHOUT TIME ZONE',
                'ALTER TABLE "foo" ALTER "bar" DROP DEFAULT',
                'COMMENT ON COLUMN "foo"."bar" IS \'(DC2Type:datetime_immutable)\'',
            ],
            $this->_platform->getAlterTableSQL($tableDiff)
        );
    }

    /**
     * {@inheritdoc}
     */
    protected function getGeneratesAlterTableRenameIndexUsedByForeignKeySQL()
    {
        return ['ALTER INDEX idx_foo RENAME TO idx_foo_renamed'];
    }

    /**
     * @group DBAL-1142
     */
    public function testInitializesTsvectorTypeMapping()
    {
        self::assertTrue($this->platform->hasDoctrineTypeMappingFor('tsvector'));
        self::assertEquals('text', $this->platform->getDoctrineTypeMapping('tsvector'));
    }

    /**
     * @group DBAL-1220
     */
    public function testReturnsDisallowDatabaseConnectionsSQL()
    {
        self::assertSame(
            "UPDATE pg_database SET datallowconn = 'false' WHERE datname = 'foo'",
            $this->platform->getDisallowDatabaseConnectionsSQL('foo')
        );
    }

    /**
     * @group DBAL-1220
     */
    public function testReturnsCloseActiveDatabaseConnectionsSQL()
    {
        self::assertSame(
            "SELECT pg_terminate_backend(procpid) FROM pg_stat_activity WHERE datname = 'foo'",
            $this->platform->getCloseActiveDatabaseConnectionsSQL('foo')
        );
    }

    /**
     * @group DBAL-2436
     */
    public function testQuotesTableNameInListTableForeignKeysSQL()
    {
        self::assertContains("'Foo''Bar\\'", $this->platform->getListTableForeignKeysSQL("Foo'Bar\\"), '', true);
    }

    /**
     * @group DBAL-2436
     */
    public function testQuotesSchemaNameInListTableForeignKeysSQL()
    {
        self::assertContains(
            "'Foo''Bar\\'",
            $this->platform->getListTableForeignKeysSQL("Foo'Bar\\.baz_table"),
            '',
            true
        );
    }

    /**
     * @group DBAL-2436
     */
    public function testQuotesTableNameInListTableConstraintsSQL()
    {
        self::assertContains("'Foo''Bar\\'", $this->platform->getListTableConstraintsSQL("Foo'Bar\\"), '', true);
    }

    /**
     * @group DBAL-2436
     */
    public function testQuotesTableNameInListTableIndexesSQL()
    {
        self::assertContains("'Foo''Bar\\'", $this->platform->getListTableIndexesSQL("Foo'Bar\\"), '', true);
    }

    /**
     * @group DBAL-2436
     */
    public function testQuotesSchemaNameInListTableIndexesSQL()
    {
        self::assertContains(
            "'Foo''Bar\\'",
            $this->platform->getListTableIndexesSQL("Foo'Bar\\.baz_table"),
            '',
            true
        );
    }

    /**
     * @group DBAL-2436
     */
    public function testQuotesTableNameInListTableColumnsSQL()
    {
        self::assertContains("'Foo''Bar\\'", $this->platform->getListTableColumnsSQL("Foo'Bar\\"), '', true);
    }

    /**
     * @group DBAL-2436
     */
    public function testQuotesSchemaNameInListTableColumnsSQL()
    {
        self::assertContains(
            "'Foo''Bar\\'",
            $this->platform->getListTableColumnsSQL("Foo'Bar\\.baz_table"),
            '',
            true
        );
    }

    /**
     * @group DBAL-2436
     */
    public function testQuotesDatabaseNameInCloseActiveDatabaseConnectionsSQL()
    {
        self::assertContains(
            "'Foo''Bar\\'",
            $this->platform->getCloseActiveDatabaseConnectionsSQL("Foo'Bar\\"),
            '',
            true
        );
    }
}
