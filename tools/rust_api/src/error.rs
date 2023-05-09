use std::fmt;

#[derive(Debug)]
pub enum Error {
    CxxException(cxx::Exception),
    FailedQuery(String),
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        use Error::*;
        match self {
            CxxException(cxx) => write!(f, "{cxx}"),
            FailedQuery(message) => write!(f, "Query execution failed: {message}"),
        }
    }
}

impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        use Error::*;
        match self {
            CxxException(cxx) => Some(cxx),
            FailedQuery(message) => None,
        }
    }
}

impl From<cxx::Exception> for Error {
    fn from(item: cxx::Exception) -> Self {
        Error::CxxException(item)
    }
}
