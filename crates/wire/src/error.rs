/// python-matter-server compatible error codes (see design spec, Error handling).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ServerErrorCode {
    UnknownError,
    NodeCommissionFailed,
    NodeInterviewFailed,
    NodeNotReady,
    NodeNotResolving,
    NodeNotExists,
    VersionMismatch,
    SdkStackError,
    InvalidArguments,
    InvalidCommand,
    UpdateCheckError,
    UpdateError,
    IcdMultiAdmin,
    OtaUploadError,
}

impl ServerErrorCode {
    pub fn code(self) -> u16 {
        match self {
            Self::UnknownError => 0,
            Self::NodeCommissionFailed => 1,
            Self::NodeInterviewFailed => 2,
            Self::NodeNotReady => 3,
            Self::NodeNotResolving => 4,
            Self::NodeNotExists => 5,
            Self::VersionMismatch => 6,
            Self::SdkStackError => 7,
            Self::InvalidArguments => 8,
            Self::InvalidCommand => 9,
            Self::UpdateCheckError => 10,
            Self::UpdateError => 11,
            Self::IcdMultiAdmin => 100,
            Self::OtaUploadError => 101,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn codes_match_python_matter_server() {
        assert_eq!(ServerErrorCode::UnknownError.code(), 0);
        assert_eq!(ServerErrorCode::NodeCommissionFailed.code(), 1);
        assert_eq!(ServerErrorCode::NodeInterviewFailed.code(), 2);
        assert_eq!(ServerErrorCode::NodeNotReady.code(), 3);
        assert_eq!(ServerErrorCode::NodeNotResolving.code(), 4);
        assert_eq!(ServerErrorCode::NodeNotExists.code(), 5);
        assert_eq!(ServerErrorCode::VersionMismatch.code(), 6);
        assert_eq!(ServerErrorCode::SdkStackError.code(), 7);
        assert_eq!(ServerErrorCode::InvalidArguments.code(), 8);
        assert_eq!(ServerErrorCode::InvalidCommand.code(), 9);
        assert_eq!(ServerErrorCode::UpdateCheckError.code(), 10);
        assert_eq!(ServerErrorCode::UpdateError.code(), 11);
        assert_eq!(ServerErrorCode::IcdMultiAdmin.code(), 100);
        assert_eq!(ServerErrorCode::OtaUploadError.code(), 101);
    }
}
