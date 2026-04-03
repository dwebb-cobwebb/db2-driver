<?php

namespace BWICompanies\DB2Driver\Schema;

use Closure;
use Illuminate\Database\Schema\Blueprint;
use Illuminate\Database\Schema\Builder;

class DB2Builder extends Builder
{
    /**
     * Determine if the given table exists.
     */
    public function hasTable($table): bool
    {
        //DWMOD - the compileTableExists method in DB2SchemaGrammar has been updated to accept the schema as a parameter, so we need to pass the schema when calling it
        //$sql = $this->grammar->compileTableExists();
        $sql = $this->grammar->compileTableExists($this->connection->getConfig('schema'), $table);
        $schemaTable = explode('.', $table);


        if (count($schemaTable) > 1) {
            $schema = $schemaTable[0];
            $table = $this->connection->getTablePrefix().$schemaTable[1];
        } else {
            $schema = $this->connection->getDefaultSchema();
            $table = $this->connection->getTablePrefix().$table;
        }


        //DWMOD - we need to pass the schema as a parameter to the query, and we can get it from the connection config or from the table name if it is specified in the table name
        //return count($this->connection->select($sql, [
        //    $schema,
        //    $table,
        //])) > 0;
        return count($this->connection->select($sql, [
            $this->connection->getConfig('schema'),
            $table
        ])) > 0;
    }

    //DWMOD - add a hasCol
    public function hasColumn($table, $column)
    {
        $schema = $this->connection->getConfig('schema');
        
        // Pass all 3 required arguments to the grammar
        $sql = $this->grammar->compileColumnExists($schema, $table, $column);

        return count($this->connection->select($sql, [
            strtoupper($schema), 
            strtoupper($table), 
            strtoupper($column)
        ])) > 0;
    }

    /**
     * Get the column listing for a given table.
     */
    //DWMOD - the getColumnListing method in the parent class has been updated to accept the connection and grammar as parameters, so we need to update the function signature to match it and use the passed arguments instead of the class properties
    // public function getColumnListing($table): array
    // {
    //     $sql = $this->grammar->compileColumnExists();
    //     $database = $this->connection->getDatabaseName();
    //     $table = $this->connection->getTablePrefix().$table;

    //     $tableExploded = explode('.', $table);

    //     if (count($tableExploded) > 1) {
    //         $database = $tableExploded[0];
    //         $table = $tableExploded[1];
    //     }

    //     $results = $this->connection->select($sql, [
    //         $database,
    //         $table,
    //     ]);

    //     $res = $this->connection->getPostProcessor()
    //                             ->processColumnListing($results);

    //     return array_values(array_map(function ($r) {
    //         return $r->column_name;
    //     }, $res));
    // }
    public function getColumnListing($table): array
    {
        $schema = $this->connection->getConfig('schema');
        
        // 1. Use the correct grammar method for a LISTING, not an EXISTS check
        $sql = $this->grammar->compileColumnListing($schema, $table);

        // 2. Handle potential table prefixes or exploded names
        $database = $this->connection->getDatabaseName();
        $table = $this->connection->getTablePrefix().$table;
        $tableExploded = explode('.', $table);

        if (count($tableExploded) > 1) {
            $database = $tableExploded[0];
            $table = $tableExploded[1];
        }

        // 3. Bind only the Schema and Table (Upper-cased for DB2)
        $results = $this->connection->select($sql, [
            strtoupper($database),
            strtoupper($table)
        ]);

        // 4. Let the PostProcessor handle the mapping to column names
        return $this->connection->getPostProcessor()->processColumnListing($results);
    }


    /**
     * Drop all tables from the current schema.
     *
     * Referential constraints (foreign keys) are removed first so that FK
     * dependencies do not block the subsequent DROP TABLE statements.
     */
    public function dropAllTables(): void
    {
        $schema = strtoupper($this->connection->getDefaultSchema());

        $tables = $this->connection->select(
            "select table_name from information_schema.tables
              where table_schema = ? and table_type = 'BASE TABLE'",
            [$schema]
        );

        if (empty($tables)) {
            return;
        }

        // Drop all foreign-key constraints before dropping tables so that
        // referential integrity does not prevent the DROP TABLE statements.
        foreach ($tables as $table) {
            $constraints = $this->connection->select(
                "select constraint_name from information_schema.table_constraints
                  where table_schema = ? and table_name = ? and constraint_type = 'FOREIGN KEY'",
                [$schema, strtoupper($table->table_name)]
            );

            foreach ($constraints as $constraint) {
                $this->connection->statement(
                    "ALTER TABLE {$schema}.{$table->table_name}
                     DROP FOREIGN KEY {$constraint->constraint_name}"
                );
            }
        }

        foreach ($tables as $table) {
            $this->connection->statement("DROP TABLE {$schema}.{$table->table_name}");
        }
    }

    /**
     * Execute the blueprint to build / modify the table.
     */
    protected function build(Blueprint $blueprint)
    {
        $schemaTable = explode('.', $blueprint->getTable());

        if (count($schemaTable) > 1) {
            $this->connection->setCurrentSchema($schemaTable[0]);
        }

        $blueprint->build($this->connection, $this->grammar);
        $this->connection->resetCurrentSchema();
    }

    /**
     * Create a new command set with a Closure.
     */
    //DWMOD - the createBlueprint method in the parent class has been updated to accept the connection as a parameter, so we need to pass the connection when creating a new blueprint instance
    // protected function createBlueprint($table, Closure $callback = null)
    // {
    //     if (isset($this->resolver)) {
    //         return call_user_func($this->resolver, $table, $callback);
    //     }

    //     return new DB2Blueprint($table, $callback);
    // }
    protected function createBlueprint($table, Closure $callback = null)
    {
        // Pass $this->connection as the first argument
        return new DB2Blueprint($this->connection, $table, $callback);
    }
}
