<?php

namespace BWICompanies\DB2Driver;

use Illuminate\Database\Query\Builder;

class DB2QueryBuilder extends Builder
{
    /**
     * Insert new records or update the existing ones using a DB2 MERGE statement.
     *
     * DB2 for i refuses to prepare any MERGE...USING (...) statement that contains
     * parameter markers (?) inside the USING subquery — regardless of whether they
     * are bare or wrapped in CAST(...) — raising SQL0584 or SQL0418 at SQLPrepare
     * time.  The only viable workaround is to inline all values directly into the
     * SQL string as properly-escaped literals, then execute with no bindings.
     *
     * @param  array  $values
     * @param  array|string  $uniqueBy
     * @param  array|null  $update
     * @return int
     */
    public function upsert(array $values, $uniqueBy, $update = null): int
    {
        if (empty($values)) {
            return 0;
        }

        if (! is_array(reset($values))) {
            $values = [$values];
        } else {
            foreach ($values as $key => $value) {
                ksort($value);
                $values[$key] = $value;
            }
        }

        if (is_null($update)) {
            $update = array_keys(reset($values));
        }

        $this->applyBeforeQueryCallbacks();

        $sql = $this->grammar->compileUpsert(
            $this,
            $values,
            array_values((array) $uniqueBy),
            is_array($update) ? $update : [$update]
        );

        // All values are inlined into $sql — pass no bindings to PDO.
        return $this->connection->affectingStatement($sql, []);
    }
}
