<?php

namespace BWICompanies\DB2Driver;

use BWICompanies\DB2Driver\Schema\DB2Builder;
use BWICompanies\DB2Driver\Schema\DB2SchemaGrammar;
use Illuminate\Database\Connection;
use PDO;

class DB2Connection extends Connection
{
    /**
     * The name of the default schema.
     */
    protected $defaultSchema;

    /**
     * The name of the current schema in use.
     */
    protected $currentSchema;

    public function __construct(
        PDO $pdo,
        string $database = '',
        string $tablePrefix = '',
        array $config = []
        ) {
        parent::__construct($pdo, $database, $tablePrefix, $config);
        $this->currentSchema = $this->defaultSchema = strtoupper($config['schema'] ?? null);

        //DWMOD - we need to set the PDO case attribute to lower case to ensure that column names are returned in lower case, which is the default behavior in Laravel, and it prevents issues with column name case sensitivity when working with DB2, which returns column names in upper case by default
        $this->pdo->setAttribute(\PDO::ATTR_CASE, \PDO::CASE_LOWER);
    }

    /**
     * Get the name of the default schema.
     */
    public function getDefaultSchema(): string
    {
        return $this->defaultSchema;
    }

    /**
     * Reset to default the current schema.
     */
    public function resetCurrentSchema()
    {
        $this->setCurrentSchema($this->getDefaultSchema());
    }

    /**
     * Set the name of the current schema.
     */
    public function setCurrentSchema(string $schema)
    {
        $this->statement('SET SCHEMA ?', [$schema !== "" ? strtoupper($schema) : "DEFAULT"]);
    }

    /**
     * Execute a system command on IBMi.
     */
    public function executeCommand($command)
    {
        $this->statement('CALL QSYS2.QCMDEXC(?)', [$command]);
    }

    /**
     * Get a schema builder instance for the connection.
     */
    public function getSchemaBuilder()
    {
        if (is_null($this->schemaGrammar)) {
            $this->useDefaultSchemaGrammar();
        }

        return new DB2Builder($this);
    }

    /**
     * @return \Illuminate\Database\Grammar
     */
    protected function getDefaultQueryGrammar()
    {
        //DWMOD - DB2QueryGrammar constructor requires a reference to the connection, so we need to pass $this in the constructor
        //$defaultGrammar = new DB2QueryGrammar
        $defaultGrammar = new DB2QueryGrammar($this);

        // If a date format was specified in constructor
        if (array_key_exists('date_format', $this->config)) {
            $defaultGrammar->setDateFormat($this->config['date_format']);
        }

        // If offset compatability mode was specified in constructor
        if (array_key_exists('offset_compatibility_mode', $this->config)) {
            $defaultGrammar->setOffsetCompatibilityMode($this->config['offset_compatibility_mode']);
        }

        //DWMOD - we are not using table prefixes, and the withTablePrefix method is not implemented in the DB2QueryGrammar class, so we will just return the default grammar without applying a table prefix
        //return $this->withTablePrefix($defaultGrammar);
        return $defaultGrammar;
    }

    /**
     * Get the efault grammar for specified Schema
     */
    protected function getDefaultSchemaGrammar(): \Illuminate\Database\Grammar
    {
        //DWMOD DB2SchemaGrammar constructor requires a reference to the connection, so we need to pass $this in the constructor
        //return new DB2SchemaGrammar;
        return new DB2SchemaGrammar($this);
    }

    /**
     * Get a new query builder instance for the connection.
     *
     * Returns DB2QueryBuilder so that upsert() can inline values directly into
     * the SQL, bypassing PDO parameter binding which DB2 for i forbids inside
     * MERGE...USING subqueries (SQL0584 / SQL0418).
     */
    public function query(): DB2QueryBuilder
    {
        return new DB2QueryBuilder(
            $this,
            $this->getQueryGrammar(),
            $this->getPostProcessor()
        );
    }

    /**
     * Get the default post processor instance
     */
    protected function getDefaultPostProcessor(): \Illuminate\Database\Query\Processors\Processor
    {
        return new DB2Processor;
    }
}
