// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

use std::time::{Duration, Instant};

use engine_store_ffi::ffi::interfaces_ffi::{BaseBuffView, RaftStoreProxyPtr, RawVoidPtr};
use futures::{compat::Future01CompatExt, executor::block_on};
use kvproto::diagnosticspb::{ServerInfoRequest, ServerInfoResponse, ServerInfoType};
use protobuf::Message;
use tikv::server::service::diagnostics::{sys, SYS_INFO};
use tikv_util::{
    sys::{ioload, SystemExt},
    timer::GLOBAL_TIMER_HANDLE,
};
use std::pin::Pin;

fn server_info_for_ffi(req: ServerInfoRequest) -> ServerInfoResponse {
    let tp = req.get_tp();

    let collect = async move {
        let (load, when) = match tp {
            ServerInfoType::LoadInfo | ServerInfoType::All => {
                let mut system = SYS_INFO.lock().unwrap();
                system.refresh_networks_list();
                system.refresh_all();
                let load = (
                    sys::cpu_time_snapshot(),
                    system
                        .networks()
                        .into_iter()
                        .map(|(n, d)| (n.to_owned(), sys::NicSnapshot::from_network_data(d)))
                        .collect(),
                    ioload::IoLoad::snapshot(),
                );
                let when = Instant::now() + Duration::from_millis(1000);
                (Some(load), when)
            }
            _ => (None, Instant::now()),
        };

        let timer = GLOBAL_TIMER_HANDLE.clone();
        let _ = timer.delay(when).compat().await;

        let mut server_infos = Vec::new();
        match req.get_tp() {
            ServerInfoType::HardwareInfo => sys::hardware_info(&mut server_infos),
            ServerInfoType::LoadInfo => sys::load_info(load.unwrap(), &mut server_infos),
            ServerInfoType::SystemInfo => sys::system_info(&mut server_infos),
            ServerInfoType::All => {
                sys::hardware_info(&mut server_infos);
                sys::load_info(load.unwrap(), &mut server_infos);
                sys::system_info(&mut server_infos);
            }
        };
        server_infos.sort_by(|a, b| (a.get_tp(), a.get_name()).cmp(&(b.get_tp(), b.get_name())));
        let mut resp = ServerInfoResponse::default();
        resp.set_items(server_infos.into());
        resp
    };

    block_on(collect)
}

pub extern "C" fn ffi_server_info(
    proxy_ptr: RaftStoreProxyPtr,
    view: BaseBuffView,
    res: RawVoidPtr,
) -> u32 {
    assert!(!proxy_ptr.is_null());
    let mut req = ServerInfoRequest::default();
    assert_ne!(view.data, std::ptr::null());
    assert_ne!(view.len, 0);
    req.merge_from_bytes(view.to_slice()).unwrap();

    let resp = server_info_for_ffi(req);
    engine_store_ffi::ffi::set_server_info_resp(&resp, res);
    0
}

pub extern "C" fn ffi_server_info_noproxy(
    view: BaseBuffView,
    res: RawVoidPtr,
) -> u32 {
    let mut req = ServerInfoRequest::default();
    assert_ne!(view.data, std::ptr::null());
    assert_ne!(view.len, 0);
    req.merge_from_bytes(view.to_slice()).unwrap();

    let resp = server_info_for_ffi(req);
    let buff = engine_store_ffi::ffi::ProtoMsgBaseBuff::new(&resp);
    let buff_ptr: BaseBuffView = Pin::new(&buff).into();
    // self.set_pb_msg_by_bytes(
    //     interfaces_ffi::MsgPBType::ServerInfoResponse,
    //     res,
    //     Pin::new(&buff).into(),
    //     )
    unsafe {
    let v = &mut *(res as *mut kvproto::diagnosticspb::ServerInfoResponse);
    v.merge_from_bytes(buff_ptr.to_slice()).unwrap();
    }
    // engine_store_ffi::ffi::set_server_info_resp(&resp, res);
    0
}
