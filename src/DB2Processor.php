<?php

namespace BWICompanies\DB2Driver;

use BWICompanies\DB2Driver\DB2QueryGrammar;
use Illuminate\Database\Query\Builder;
use Illuminate\Database\Query\Processors\Processor;

class DB2Processor extends Processor
{
    public function processInsertGetId(Builder $query, $sql, $values, $sequence = null)
    {
        $sequenceStr = $sequence ?: 'id';

        if (is_array($sequence)) {
            $grammar = new DB2QueryGrammar;
            $sequenceStr = $grammar->columnize($sequence);
        }

        $sqlStr = 'select %s from new table (%s)';

        $finalSql = sprintf($sqlStr, $sequenceStr, $sql);
        $results = $query->getConnection()
                         ->select($finalSql, $values);

        if (is_array($sequence)) {
            return array_values((array) $results[0]);
        } else {
            $result = (array) $results[0];
            if (isset($result[$sequenceStr])) {
                $id = $result[$sequenceStr];
            } else {
                $id = $result[strtoupper($sequenceStr)];
            }

            return is_numeric($id) ? (int) $id : $id;
        }
    }

    /**
     * Process the results of a "select" query.
     *
     * Reverses the STX + hex encoding applied by DB2Connection::bindValues() for
     * strings that originally contained null bytes (e.g. PHP serialize() output).
     * Any column value that starts with \x02 followed by an even-length string of
     * valid hex characters is decoded via hex2bin().  All other values are returned
     * unchanged.
     *
     * @param  \Illuminate\Database\Query\Builder  $query
     * @param  array  $results
     * @return array
     */
    public function processSelect(Builder $query, $results)
    {
        return array_map(function ($row) {
            foreach (get_object_vars($row) as $key => $value) {
                if (
                    is_string($value)
                    && isset($value[0])
                    && $value[0] === "\x02"
                ) {
                    $hex = substr($value, 1);
                    if (strlen($hex) % 2 === 0 && ctype_xdigit($hex)) {
                        $row->$key = hex2bin($hex);
                    }
                }
            }

            return $row;
        }, $results);
    }

    /**
     * Process the results of a column listing query.
     * This was present in Illuminate\Database\Query\Processor.php 9.x but later removed.
     *
     * @param  array  $results
     * @return array
     */
    public function processColumnListing($results)
    {
        return $results;
    }
}