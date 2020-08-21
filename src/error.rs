use std::fmt;
use std::os::unix::io::RawFd;

pub struct Error {
    pub(crate) inner: std::io::Error,
    pub(crate) op: &'static str,
    pub(crate) path: Option<String>,
    pub(crate) fd: Option<RawFd>,
}

impl fmt::Debug for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self)
    }
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}, op: {}", self.inner, self.op)?;
        if let Some(path) = &self.path {
            write!(f, " path {}", path)?;
        }

        if let Some(fd) = &self.fd {
            write!(f, " with fd {}", fd)?;
        }
        Ok(())
    }
}

impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(&self.inner)
    }
}

impl From<Error> for std::io::Error {
    fn from(err: Error) -> std::io::Error {
        err.inner
    }
}
