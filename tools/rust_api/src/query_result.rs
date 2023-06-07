use crate::ffi::ffi;
use crate::value::Value;
use cxx::UniquePtr;
use std::convert::TryInto;
use std::fmt;

/// Stores the result of a query execution
pub struct QueryResult {
    pub(crate) result: UniquePtr<ffi::QueryResult>,
}

// TODO: getNext will throw an exception if the query result is a failure result
// Is there any need for a QueryResult at all in that case, instead of some form of error?
// If so, then we need to make sure you can't call next when the query failed.
// Otherwise, it shouldn't matter and getNext should be safe.
//
// getNumColumns, getColumnDataTypes, getColumnNames, getNumTuples can be called when it's an error
//
// Even if that data is needed, we could use a custom error type that retains it
impl QueryResult {
    pub fn display(&mut self) -> String {
        ffi::query_result_to_string(self.result.pin_mut())
    }
}

// the underlying C++ type is both data and an iterator (sort-of)
impl Iterator for QueryResult {
    // we will be counting with usize
    type Item = Vec<Value>;

    // next() is the only required method
    fn next(&mut self) -> Option<Self::Item> {
        if ffi::query_result_has_next(self.result.as_ref().unwrap()) {
            let flat_tuple = ffi::query_result_get_next(self.result.pin_mut());
            let mut result = vec![];
            for i in 0..ffi::flat_tuple_len(flat_tuple.as_ref().unwrap()) {
                let value = ffi::flat_tuple_get_value(flat_tuple.as_ref().unwrap(), i);
                // TODO: Return result instead of unwrapping?
                // Unfortunately, as an iterator, this would require producing
                // Vec<Result<Value>>, though it would be possible to turn that into
                // Result<Vec<Value>> instead, but it would lose information when multiple failures
                // occur.
                result.push(value.try_into().unwrap());
            }
            Some(result)
        } else {
            None
        }
    }
}

impl fmt::Debug for QueryResult {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "QueryResult(can't be displayed without mutation!)")
    }
}

/* TODO: QueryResult.toString() needs to be const
impl std::fmt::Display for QueryResult {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", ffi::query_result_to_string(self.result.as_ref().unwrap()))
    }
}
*/
