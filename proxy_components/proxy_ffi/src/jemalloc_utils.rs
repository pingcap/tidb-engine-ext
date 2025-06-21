// Copyright 2024 TiKV Project Authors. Licensed under Apache-2.0.
use std::sync::Mutex;

extern "C" {
    // External jemalloc
    pub fn mallctl(
        name: *const ::std::os::raw::c_char,
        oldp: *mut ::std::os::raw::c_void,
        oldlenp: *mut u64,
        newp: *mut ::std::os::raw::c_void,
        newlen: u64,
    ) -> ::std::os::raw::c_int;

    // Embedded jemalloc
    pub fn _rjem_mallctl(
        name: *const ::std::os::raw::c_char,
        oldp: *mut ::std::os::raw::c_void,
        oldlenp: *mut u64,
        newp: *mut ::std::os::raw::c_void,
        newlen: u64,
    ) -> ::std::os::raw::c_int;

    pub fn malloc_stats_print(
        write_cb: Option<unsafe extern "C" fn(*mut c_void, *const ::std::os::raw::c_char)>,
        cbopaque: *mut c_void,
        opts: *const ::std::os::raw::c_char,
    );
}

#[allow(unused_variables)]
#[allow(unused_mut)]
#[allow(unused_unsafe)]
#[allow(unreachable_code)]
pub fn issue_mallctl_args(
    command: &str,
    oldptr: *mut ::std::os::raw::c_void,
    oldsize: *mut u64,
    newptr: *mut ::std::os::raw::c_void,
    newsize: u64,
) -> ::std::os::raw::c_int {
    unsafe {
        let c_str = std::ffi::CString::new(command).unwrap();
        let c_ptr: *const ::std::os::raw::c_char = c_str.as_ptr() as *const ::std::os::raw::c_char;
        // See unprefixed_malloc_on_supported_platforms in tikv-jemalloc-sys.
        #[cfg(any(test, feature = "testexport"))]
        {
            // Test part
            #[cfg(feature = "jemalloc")]
            {
                // See NO_UNPREFIXED_MALLOC
                #[cfg(any(target_os = "android", target_os = "dragonfly", target_os = "macos"))]
                return _rjem_mallctl(c_ptr, oldptr, oldsize, newptr, newsize);
                #[cfg(not(any(
                    target_os = "android",
                    target_os = "dragonfly",
                    target_os = "macos"
                )))]
                return mallctl(c_ptr, oldptr, oldsize, newptr, newsize);
            }
            0
        }

        #[cfg(not(any(test, feature = "testexport")))]
        {
            // No test part
            #[cfg(feature = "external-jemalloc")]
            {
                // Must linked to tiflash.
                return mallctl(c_ptr, oldptr, oldsize, newptr, newsize);
            }
            #[cfg(not(feature = "external-jemalloc"))]
            {
                // Happens only with `raftstore-proxy-main`
                #[cfg(not(any(
                    target_os = "android",
                    target_os = "dragonfly",
                    target_os = "macos"
                )))]
                {
                    return mallctl(c_ptr, oldptr, oldsize, newptr, newsize);
                }
                0
            }
        }
    }
}

#[allow(unused_variables)]
#[allow(unused_mut)]
#[allow(unused_unsafe)]
pub fn issue_mallctl(command: &str) -> u64 {
    type PtrUnderlying = u64;
    let mut ptr: PtrUnderlying = 0;
    let mut size = std::mem::size_of::<PtrUnderlying>() as u64;
    issue_mallctl_args(
        command,
        &mut ptr as *mut _ as *mut ::std::os::raw::c_void,
        &mut size as *mut u64,
        std::ptr::null_mut(),
        0,
    );
    ptr
}

pub fn get_allocatep_on_thread_start() -> u64 {
    issue_mallctl("thread.allocatedp")
}

pub fn get_deallocatep_on_thread_start() -> u64 {
    issue_mallctl("thread.deallocatedp")
}

pub fn get_allocate() -> u64 {
    issue_mallctl("thread.allocated")
}

pub fn get_deallocate() -> u64 {
    issue_mallctl("thread.deallocated")
}

use std::ffi::{c_char, c_void, CStr};
struct CaptureContext {
    buffer: Mutex<String>,
}

#[allow(dead_code)]
extern "C" fn write_to_string(ctx: *mut c_void, message: *const c_char) {
    if ctx.is_null() || message.is_null() {
        return;
    }

    let context = unsafe { &*(ctx as *mut CaptureContext) };

    let c_str = unsafe { CStr::from_ptr(message) };
    if let Ok(str_slice) = c_str.to_str() {
        let mut buffer = context.buffer.lock().unwrap();
        buffer.push_str(str_slice);
    }
}

#[allow(unused_variables)]
#[allow(unused_mut)]
#[allow(unused_unsafe)]
pub fn get_malloc_stats() -> String {
    let context = CaptureContext {
        buffer: Mutex::new(String::new()),
    };

    // Use Json format.
    let c_str = std::ffi::CString::new("J").unwrap();
    let ops_str = c_str.as_ptr() as *const std::os::raw::c_char;

    unsafe {
        // See unprefixed_malloc_on_supported_platforms in tikv-jemalloc-sys.
        #[cfg(any(test, feature = "testexport"))]
        {
            // Test part
            #[cfg(feature = "jemalloc")]
            {
                // See NO_UNPREFIXED_MALLOC
                #[cfg(any(target_os = "android", target_os = "dragonfly", target_os = "macos"))]
                _rjem_malloc_stats_print(
                    Some(write_to_string),
                    &context as *const _ as *mut c_void,
                    ops_str,
                );
                #[cfg(not(any(
                    target_os = "android",
                    target_os = "dragonfly",
                    target_os = "macos"
                )))]
                malloc_stats_print(
                    Some(write_to_string),
                    &context as *const _ as *mut c_void,
                    ops_str,
                );
            }
        }

        #[cfg(not(any(test, feature = "testexport")))]
        {
            // No test part
            #[cfg(feature = "external-jemalloc")]
            {
                // Must linked to tiflash.
                malloc_stats_print(
                    Some(write_to_string),
                    &context as *const _ as *mut c_void,
                    ops_str,
                );
            }
            #[cfg(not(feature = "external-jemalloc"))]
            {
                // Happens only with `raftstore-proxy-main`
                #[cfg(not(any(
                    target_os = "android",
                    target_os = "dragonfly",
                    target_os = "macos"
                )))]
                {
                    malloc_stats_print(
                        Some(write_to_string),
                        &context as *const _ as *mut c_void,
                        ops_str,
                    );
                }
            }
        }
    }

    let buffer = context.buffer.lock().unwrap();
    buffer.clone()
}
