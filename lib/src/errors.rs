use bolt_client::bolt_proto::error::ConversionError;
use bolt_client::error::CommunicationError;

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug, PartialEq)]
pub enum Error {
    IOError { detail: String },
    ConnectionError,
    StringTooLong,
    MapTooBig,
    BytesTooBig,
    ListTooLong,
    InvalidConfig,
    UnsupportedVersion(String),
    UnexpectedMessage(String),
    UnknownType(String),
    UnknownMessage(String),
    ConversionError,
    AuthenticationError(String),
    InvalidTypeMarker(String),
    DeserializationError(String),
    CommunicationError
}

impl std::convert::From<std::io::Error> for Error {
    fn from(e: std::io::Error) -> Self {
        Error::IOError {
            detail: e.to_string(),
        }
    }
}

impl std::convert::From<deadpool::managed::PoolError<Error>> for Error {
    fn from(e: deadpool::managed::PoolError<Error>) -> Self {
        match e {
            deadpool::managed::PoolError::Backend(e) => e,
            _ => Error::ConnectionError,
        }
    }
}

impl From<CommunicationError> for Error {
    fn from(error: CommunicationError) -> Self {
        Error::CommunicationError
    }
}

impl From<ConversionError> for Error {
    fn from(error: ConversionError) -> Self {
        Error::CommunicationError
    }
}


pub fn unexpected<T: std::fmt::Debug>(response: T, request: &str) -> Error {
    Error::UnexpectedMessage(format!(
        "unexpected response for {}: {:?}",
        request, response
    ))
}
