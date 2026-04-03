<?php

namespace BWICompanies\DB2Driver;

use Illuminate\Database\Query\Builder;

class DB2QueryBuilder extends Builder
{
    /**
     * Insert new records or update the existing ones.
     *
     * DB2 for i prohibits parameter markers (?) anywhere inside the USING subquery
     * of a MERGE statement (SQL0584 / SQL0418), and inlining values as SQL literals
     * breaks on strings that contain null bytes (e.g. PHP-serialised objects store
     * protected property names as \x00*\x00name), causing SQL0010.
     *
     * The pragmatic solution is to implement upsert as a per-row UPDATE followed by
     * an INSERT when no row was matched.  Both statements use normal PDO parameter
     * binding, which handles arbitrary binary data transparently.  The sequence is
     * not atomic, but that is acceptable for the cache and session use-cases that
     * drive this method; callers with strict atomicity requirements should use a
     * DB2 transaction around the call.
     *
     * @param  array  $values
     * @param  array|string  $uniqueBy
     * @param  array|null  $update
     * @return int
     */
    /**
     * Insert new records, ignoring rows that would violate a unique constraint.
     *
     * DB2 has no native INSERT OR IGNORE / INSERT IGNORE syntax, and the base
     * Grammar throws a RuntimeException for compileInsertOrIgnore().  We
     * implement the semantics by attempting a normal INSERT per row and silently
     * swallowing unique-constraint violations (SQLSTATE 23505 / SQL0803).
     *
     * @param  array  $values
     * @return int  Number of rows actually inserted.
     */
    public function insertOrIgnore(array $values): int
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
        $this->applyBeforeQueryCallbacks();
        $affected = 0;
        foreach ($values as $record) {
            try {
                $this->newQuery()->from($this->from)->insert($record);
                $affected++;
            } catch (\Illuminate\Database\QueryException $e) {
                // SQLSTATE 23505 = unique constraint violation (DB2 SQL0803)
                // SQLSTATE 23000 = integrity constraint violation (general)
                if (! in_array($e->getCode(), ['23505', '23000'])) {
                    throw $e;
                }
            }
        }
        return $affected;
    }
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

        $update   = is_array($update) ? $update : [$update];
        $uniqueBy = array_values((array) $uniqueBy);

        $this->applyBeforeQueryCallbacks();

        $affected = 0;

        foreach ($values as $record) {
            // Resolve the SET columns/values for the UPDATE.
            $updateData = [];
            foreach ($update as $key => $col) {
                if (is_numeric($key)) {
                    // Plain column name — pull the value from the current record.
                    $updateData[$col] = $record[$col];
                } else {
                    // key => literal-value override supplied by the caller.
                    $updateData[$key] = $col;
                }
            }

            // Attempt to UPDATE any row(s) that match the unique key columns.
            $updateBuilder = $this->newQuery()->from($this->from);
            foreach ($uniqueBy as $col) {
                $updateBuilder->where($col, $record[$col]);
            }

            $rowsUpdated = $updateBuilder->update($updateData);

            if ($rowsUpdated > 0) {
                $affected += $rowsUpdated;
            } else {
                // Nothing matched — INSERT the full record.
                $this->newQuery()->from($this->from)->insert($record);
                $affected++;
            }
        }

        return $affected;
    }
}
