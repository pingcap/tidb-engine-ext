// Copyright 2024 TiKV Project Authors. Licensed under Apache-2.0.

#[allow(unused_variables)]
#[allow(unreachable_code)]
pub fn issue_mallctl_args(
    _command: &str,
    _oldptr: *mut ::std::os::raw::c_void,
    _oldsize: *mut u64,
    _newptr: *mut ::std::os::raw::c_void,
    _newsize: u64,
) -> ::std::os::raw::c_int {
    0
}

#[allow(unused_variables)]
#[allow(unused_mut)]
#[allow(unused_unsafe)]
pub fn issue_mallctl(command: &str) -> u64 {
    0
}

pub fn get_allocatep_on_thread_start() -> u64 {
    0
}

pub fn get_deallocatep_on_thread_start() -> u64 {
    0
}

pub fn get_allocate() -> u64 {
    0
}

pub fn get_deallocate() -> u64 {
    0
}

pub fn get_malloc_stats() -> String {
    String::new()
}
