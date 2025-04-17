pub mod message {
    use std::fmt;

    include!(concat!(env!("OUT_DIR"), "/message.rs"));
    impl fmt::Display for ErrCode {
        fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
            write!(f, "{:?}", self)
        }
    }

    pub trait ProtoErrorSupported {
        fn new_with_err_code(code: ErrCode) -> Self;
        fn get_err(&self) -> &Option<Error>;
    }

    macro_rules! proto_error_supported {
        ($tp:ty) => {
            impl ProtoErrorSupported for $tp {
                fn new_with_err_code(code: ErrCode) -> Self {
                    let mut instance = <$tp>::default();
                    instance.error = Some(Error {
                        code: code as i32,
                        message: code.to_string(),
                    });
                    instance
                }

                fn get_err(&self) -> &Option<Error> {
                    &self.error
                }
            }
        };
    }

    proto_error_supported!(GetResponse);
    proto_error_supported!(PrewriteResponse);
    proto_error_supported!(CommitResponse);
}
