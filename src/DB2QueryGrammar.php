<?php

namespace BWICompanies\DB2Driver;

use Illuminate\Database\Query\Builder;
use Illuminate\Database\Query\Grammars\Grammar;

class DB2QueryGrammar extends Grammar
{
    /**
     * The format for database stored dates.
     *
     * @var string
     */
    protected $dateFormat;

    /**
     * Offset compatibility mode true triggers FETCH FIRST X ROWS and ROW_NUM behavior for older versions of DB2
     *
     * @var bool
     */
    protected $offsetCompatibilityMode = true;

    /**
     * Wrap a single string in keyword identifiers.
     *
     * @param  string  $value
     * @return string
     */
    protected function wrapValue($value)
    {
        if ($value === '*') {
            return $value;
        }

        return str_replace('"', '""', $value);
    }

    /**
     * Compile the "limit" portions of the query.
     *
     * @param  \Illuminate\Database\Query\Builder  $query
     * @param  int  $limit
     * @return string
     */
    protected function compileLimit(Builder $query, $limit)
    {
        if ($this->offsetCompatibilityMode) {
            return "FETCH FIRST $limit ROWS ONLY";
        }

        return parent::compileLimit($query, $limit);
    }

    /**
     * Compile a select query into SQL.
     *
     * @param  \Illuminate\Database\Query\Builder  $query
     * @return string
     */
    public function compileSelect(Builder $query)
    {
        if (! $this->offsetCompatibilityMode) {
            return parent::compileSelect($query);
        }

        if (is_null($query->columns)) {
            $query->columns = ['*'];
        }

        $components = $this->compileComponents($query);

        // If an offset is present on the query, we will need to wrap the query in
        // a big "ANSI" offset syntax block. This is very nasty compared to the
        // other database systems but is necessary for implementing features.
        if ($query->offset > 0) {
            return $this->compileAnsiOffset($query, $components);
        }

        return $this->concatenate($components);
    }

    /**
     * Create a full ANSI offset clause for the query.
     *
     * @param  \Illuminate\Database\Query\Builder  $query
     * @param  array  $components
     * @return string
     */
    protected function compileAnsiOffset(Builder $query, $components)
    {
        // An ORDER BY clause is required to make this offset query work, so if one does
        // not exist we'll just create a dummy clause to trick the database and so it
        // does not complain about the queries for not having an "order by" clause.
        if (! isset($components['orders'])) {
            $components['orders'] = 'order by 1';
        }

        unset($components['limit']);

        // We need to add the row number to the query so we can compare it to the offset
        // and limit values given for the statements. So we will add an expression to
        // the "select" that will give back the row numbers on each of the records.
        $orderings = $components['orders'];

        $columns = (! empty($components['columns']) ? $components['columns'].', ' : 'select');

        if ($columns == 'select *, ' && $query->from) {
            $columns = 'select '.$this->tablePrefix.$query->from.'.*, ';
        }

        $components['columns'] = $this->compileOver($orderings, $columns);

        // if there are bindings in the order, we need to move them to the select since we are moving the parameter
        // markers there with the OVER statement
        if (isset($query->getRawBindings()['order'])) {
            $query->addBinding($query->getRawBindings()['order'], 'select');
            $query->setBindings([], 'order');
        }

        unset($components['orders']);

        // Next we need to calculate the constraints that should be placed on the query
        // to get the right offset and limit from our query but if there is no limit
        // set we will just handle the offset only since that is all that matters.
        $start = $query->offset + 1;

        $constraint = $this->compileRowConstraint($query);

        $sql = $this->concatenate($components);

        // We are now ready to build the final SQL query so we'll create a common table
        // expression from the query and get the records with row numbers within our
        // given limit and offset value that we just put on as a query constraint.
        return $this->compileTableExpression($sql, $constraint);
    }

    /**
     * Compile the over statement for a table expression.
     *
     * @param  string  $orderings
     * @param    $columns
     * @return string
     */
    protected function compileOver($orderings, $columns)
    {
        return "{$columns} row_number() over ({$orderings}) as row_num";
    }

    /**
     * @param $query
     * @return string
     */
    protected function compileRowConstraint($query)
    {
        $start = $query->offset + 1;

        if ($query->limit > 0) {
            $finish = $query->offset + $query->limit;

            return "between {$start} and {$finish}";
        }

        return ">= {$start}";
    }

    /**
     * Compile a common table expression for a query.
     *
     * @param  string  $sql
     * @param  string  $constraint
     * @return string
     */
    protected function compileTableExpression($sql, $constraint)
    {
        return "select * from ({$sql}) as temp_table where row_num {$constraint}";
    }

    /**
     * Compile the "offset" portions of the query.
     *
     * @param  \Illuminate\Database\Query\Builder  $query
     * @param  int  $offset
     * @return string
     */
    protected function compileOffset(Builder $query, $offset)
    {
        if ($this->offsetCompatibilityMode) {
            return '';
        }

        return parent::compileOffset($query, $offset);
    }

    /**
     * Compile an exists statement into SQL.
     *
     * @param  \Illuminate\Database\Query\Builder  $query
     * @return string
     */
    public function compileExists(Builder $query)
    {
        $existsQuery = clone $query;

        $existsQuery->columns = [];

        return $this->compileSelect($existsQuery->selectRaw('1 exists')->limit(1));
    }

    /**
     * Get the format for database stored dates.
     *
     * @return string
     */
    public function getDateFormat()
    {
        return $this->dateFormat ?? parent::getDateFormat();
    }

    /**
     * Set the format for database stored dates.
     *
     * @param $dateFormat
     */
    public function setDateFormat($dateFormat)
    {
        $this->dateFormat = $dateFormat;
    }

    /**
     * Set offset compatibility mode to trigger FETCH FIRST X ROWS and ROW_NUM behavior for older versions of DB2
     *
     * @param $bool
     */
    public function setOffsetCompatibilityMode($bool)
    {
        $this->offsetCompatibilityMode = $bool;
    }

    /**
     * Compile an upsert statement into SQL using DB2's MERGE syntax.
     *
     * @param  \Illuminate\Database\Query\Builder  $query
     * @param  array  $values
     * @param  array  $uniqueBy
     * @param  array  $update
     * @return string
     */
    /**
     * Compile an upsert statement into SQL using DB2's MERGE syntax.
     *
     * DB2 for i does not permit parameter markers (?) anywhere inside the USING
     * subquery of a MERGE statement — not even inside CAST(? AS type) — raising
     * SQL0584 or SQL0418 at SQLPrepare time.  All values are therefore inlined
     * as properly-escaped SQL literals so that the statement contains no markers
     * and can be executed with an empty bindings array.
     *
     * @see DB2QueryBuilder::upsert() — executes the result with no bindings.
     */
    public function compileUpsert(Builder $query, array $values, array $uniqueBy, array $update): string
    {
        $table = $this->wrapTable($query->from);
        $columns = array_keys(reset($values));
        $sourceAlias = 'laravel_source';

        $source = collect($values)->map(function ($record) use ($columns) {
            $selects = collect($columns)->map(function ($col) use ($record) {
                return $this->quoteInlineValue($record[$col]).' AS '.$this->wrap($col);
            })->implode(', ');

            return "SELECT {$selects} FROM SYSIBM.SYSDUMMY1";
        })->implode(' UNION ALL ');

        $on = collect($uniqueBy)->map(function ($column) use ($table, $sourceAlias) {
            $wrapped = $this->wrap($column);

            return "{$table}.{$wrapped} = {$sourceAlias}.{$wrapped}";
        })->implode(' AND ');

        $sql = "MERGE INTO {$table} USING ({$source}) AS {$sourceAlias} ON {$on}";

        if (! empty($update)) {
            $sets = collect($update)->map(function ($value, $key) use ($table, $sourceAlias) {
                if (is_numeric($key)) {
                    $wrapped = $this->wrap($value);

                    return "{$table}.{$wrapped} = {$sourceAlias}.{$wrapped}";
                }

                // Literal override value — inline it too so there are no markers.
                return $table.'.'.$this->wrap($key).' = '.$this->quoteInlineValue($value);
            })->implode(', ');

            $sql .= " WHEN MATCHED THEN UPDATE SET {$sets}";
        }

        $insertColumns = $this->columnize($columns);

        $insertValues = collect($columns)->map(function ($col) use ($sourceAlias) {
            return "{$sourceAlias}.{$this->wrap($col)}";
        })->implode(', ');

        $sql .= " WHEN NOT MATCHED THEN INSERT ({$insertColumns}) VALUES ({$insertValues})";

        return $sql;
    }

    /**
     * Render a PHP value as an inline SQL literal safe for embedding in a
     * MERGE...USING subquery on DB2 for i.
     *
     * Single-quote escaping (doubling interior apostrophes) is the standard
     * SQL mechanism and is fully supported by DB2 for i.  Raw Expression
     * instances are passed through unchanged so callers can still inject
     * hand-crafted SQL fragments when needed.
     */
    protected function quoteInlineValue(mixed $value): string
    {
        if ($this->isExpression($value)) {
            return $this->getValue($value);
        }

        if (is_null($value)) {
            return 'NULL';
        }

        if (is_bool($value)) {
            return $value ? '1' : '0';
        }

        if (is_int($value)) {
            return (string) $value;
        }

        if (is_float($value)) {
            return (string) $value;
        }

        // String: escape any embedded single quotes by doubling them, then wrap.
        return "'".str_replace("'", "''", (string) $value)."'";
    }

    /**
     * Compile the SQL statement to define a savepoint.
     *
     * @param  string  $name
     * @return string
     */
    public function compileSavepoint($name)
    {
        return 'SAVEPOINT '.$name.' ON ROLLBACK RETAIN CURSORS';
    }
}
