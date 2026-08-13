//! IM operations, one module per family. Everything here runs on the stack
//! thread and takes `&StackCtx` as its first argument.

pub(crate) mod interact;
