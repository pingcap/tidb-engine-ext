// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.
// Some clone is indeed needless. However, we ignore here for convenience.
#![allow(clippy::redundant_clone)]
use std::{
    cmp,
    convert::TryFrom,
    env, fmt,
    net::SocketAddr,
    path::{Path, PathBuf},
    str::FromStr,
    sync::{
        atomic::{AtomicBool, AtomicU32, AtomicU64, AtomicU8, Ordering},
        mpsc, Arc, Mutex,
    },
    thread,
    time::Duration,
    u64,
};

use api_version::{dispatch_api_version, KvFormat};
use concurrency_manager::ConcurrencyManager;
use encryption_export::{data_key_manager_from_config, DataKeyManager};
use engine_rocks::{
    flush_engine_statistics, from_rocks_compression_type,
    raw::{Cache, Env},
    FlowInfo, RocksEngine, RocksStatistics,
};
use engine_rocks_helper::sst_recovery::{RecoveryRunner, DEFAULT_CHECK_INTERVAL};
use engine_store_ffi::{
    self,
    core::DebugStruct,
    ffi::{
        interfaces_ffi::{
            EngineStoreServerHelper, EngineStoreServerStatus, RaftProxyStatus,
            RaftStoreProxyFFIHelper,
        },
        read_index_helper::ReadIndexClient,
        RaftStoreProxy, RaftStoreProxyFFI,
    },
    TiFlashEngine,
};
use engine_tiflash::PSLogEngine;
use engine_traits::{
    CachedTablet, CfOptionsExt, Engines, FlowControlFactorsExt, KvEngine, MiscExt, RaftEngine,
    SingletonFactory, StatisticsReporter, TabletContext, TabletRegistry, CF_DEFAULT, CF_LOCK,
    CF_WRITE,
};
use error_code::ErrorCodeExt;
use file_system::{
    get_io_rate_limiter, set_io_rate_limiter, BytesFetcher, File, IoBudgetAdjustor,
    MetricsManager as IOMetricsManager,
};
use futures::executor::block_on;
use grpcio::{EnvBuilder, Environment};
use grpcio_health::HealthService;
use kvproto::{
    debugpb::create_debug, diagnosticspb::create_diagnostics, import_sstpb::create_import_sst,
};
use pd_client::{PdClient, RpcClient};
use raft_log_engine::RaftLogEngine;
use raftstore::{
    coprocessor::{config::SplitCheckConfigManager, CoprocessorHost, RegionInfoAccessor},
    router::ServerRaftStoreRouter,
    store::{
        config::RaftstoreConfigManager,
        fsm,
        fsm::store::{
            RaftBatchSystem, RaftRouter, StoreMeta, MULTI_FILES_SNAPSHOT_FEATURE, PENDING_MSG_CAP,
        },
        memory::MEMTRACE_ROOT as MEMTRACE_RAFTSTORE,
        AutoSplitController, CheckLeaderRunner, LocalReader, SnapManager, SnapManagerBuilder,
        SplitCheckRunner, SplitConfigManager, StoreMetaDelegate,
    },
};
use resource_control::{
    ResourceGroupManager, ResourceManagerService, MIN_PRIORITY_UPDATE_INTERVAL,
};
use security::SecurityManager;
use server::{memory::*, raft_engine_switch::*};
use tikv::{
    config::{ConfigController, DbConfigManger, DbType, TikvConfig},
    coprocessor::{self, MEMTRACE_ROOT as MEMTRACE_COPROCESSOR},
    coprocessor_v2,
    import::{ImportSstService, SstImporter},
    read_pool::{build_yatp_read_pool, ReadPool, ReadPoolConfigManager},
    server::{
        config::{Config as ServerConfig, ServerConfigManager},
        gc_worker::GcWorker,
        raftkv::ReplicaReadLockChecker,
        resolve,
        service::{DebugService, DiagnosticsService},
        ttl::TtlChecker,
        KvEngineFactoryBuilder, Node, RaftKv, Server, CPU_CORES_QUOTA_GAUGE, DEFAULT_CLUSTER_ID,
        GRPC_THREAD_PREFIX,
    },
    storage::{
        self,
        config_manager::StorageConfigManger,
        txn::flow_controller::{EngineFlowController, FlowController},
        Engine, Storage,
    },
};
use tikv_util::{
    check_environment_variables,
    config::{ensure_dir_exist, RaftDataStateMachine, ReadableDuration, VersionTrack},
    error,
    math::MovingAvgU32,
    quota_limiter::{QuotaLimitConfigManager, QuotaLimiter},
    sys::{disk, register_memory_usage_high_water, thread::ThreadBuildWrapper, SysQuota},
    thread_group::GroupProperties,
    time::{Instant, Monitor},
    worker::{Builder as WorkerBuilder, LazyWorker, Scheduler, Worker},
    yatp_pool::CleanupMethod,
    Either,
};
use tokio::runtime::Builder;

use crate::{
    config::ProxyConfig, engine::ProxyRocksEngine, fatal,
    hacked_lock_mgr::HackedLockManager as LockManager, setup::*, status_server::StatusServer,
    util::ffi_server_info,
};

#[inline]
pub fn run_impl<CER: ConfiguredRaftEngine, F: KvFormat>(
    config: TikvConfig,
    proxy_config: ProxyConfig,
    engine_store_server_helper: &EngineStoreServerHelper,
) {
    let engine_store_server_helper_ptr = engine_store_server_helper as *const _ as isize;
    let mut tikv = TiKvServer::<CER>::init(config, proxy_config, engine_store_server_helper_ptr);

    // Must be called after `TiKvServer::init`.
    let memory_limit = tikv.config.memory_usage_limit.unwrap().0;
    let high_water = (tikv.config.memory_usage_high_water * memory_limit as f64) as u64;
    register_memory_usage_high_water(high_water);

    tikv.check_conflict_addr();
    tikv.init_fs();
    tikv.init_yatp();
    tikv.init_encryption();

    let mut proxy = RaftStoreProxy::new(
        AtomicU8::new(RaftProxyStatus::Idle as u8),
        tikv.encryption_key_manager.clone(),
        Some(Box::new(ReadIndexClient::new(
            tikv.router.clone(),
            SysQuota::cpu_cores_quota() as usize * 2,
        ))),
        None,
    );

    let proxy_ref = &proxy;
    let proxy_helper = {
        let mut proxy_helper = RaftStoreProxyFFIHelper::new(proxy_ref.into());
        proxy_helper.fn_server_info = Some(ffi_server_info);
        proxy_helper
    };

    info!("set raft-store proxy helper");

    engine_store_server_helper.handle_set_proxy(&proxy_helper);

    info!("wait for engine-store server to start");
    while engine_store_server_helper.handle_get_engine_store_server_status()
        == EngineStoreServerStatus::Idle
    {
        thread::sleep(Duration::from_millis(200));
    }

    if engine_store_server_helper.handle_get_engine_store_server_status()
        != EngineStoreServerStatus::Running
    {
        info!("engine-store server is not running, make proxy exit");
        return;
    }

    info!("engine-store server is started");

    let fetcher = tikv.init_io_utility();
    let listener = tikv.init_flow_receiver();
    let engine_store_server_helper_ptr = engine_store_server_helper as *const _ as isize;
    // Will call TiFlashEngine::init
    let (engines, engines_info) =
        tikv.init_tiflash_engines(listener, engine_store_server_helper_ptr);
    tikv.init_engines(engines.clone());
    {
        if engines.kv.element_engine.is_none() {
            error!("TiFlashEngine has empty ElementaryEngine");
        }
        proxy.set_kv_engine(
            engine_store_ffi::ffi::RaftStoreProxyEngine::from_tiflash_engine(engines.kv.clone()),
        );
    }
    let server_config = tikv.init_servers::<F>();
    tikv.register_services();
    tikv.init_metrics_flusher(fetcher, engines_info);
    tikv.init_storage_stats_task(engines);
    tikv.run_server(server_config);
    tikv.run_status_server();

    proxy.set_status(RaftProxyStatus::Running);

    {
        debug_assert!(
            engine_store_server_helper.handle_get_engine_store_server_status()
                == EngineStoreServerStatus::Running
        );
        let _ = tikv.engines.take().unwrap().engines;
        loop {
            if engine_store_server_helper.handle_get_engine_store_server_status()
                != EngineStoreServerStatus::Running
            {
                break;
            }
            thread::sleep(Duration::from_millis(200));
        }
    }

    info!(
        "found engine-store server status is {:?}, start to stop all services",
        engine_store_server_helper.handle_get_engine_store_server_status()
    );

    tikv.stop();

    proxy.set_status(RaftProxyStatus::Stopped);

    info!("all services in raft-store proxy are stopped");

    info!("wait for engine-store server to stop");
    while engine_store_server_helper.handle_get_engine_store_server_status()
        != EngineStoreServerStatus::Terminated
    {
        thread::sleep(Duration::from_millis(200));
    }
    info!("engine-store server is stopped");
}

#[inline]
fn run_impl_only_for_decryption<CER: ConfiguredRaftEngine, F: KvFormat>(
    config: TikvConfig,
    _proxy_config: ProxyConfig,
    engine_store_server_helper: &EngineStoreServerHelper,
) {
    let encryption_key_manager =
        data_key_manager_from_config(&config.security.encryption, &config.storage.data_dir)
            .map_err(|e| {
                panic!(
                    "Encryption failed to initialize: {}. code: {}",
                    e,
                    e.error_code()
                )
            })
            .unwrap()
            .map(Arc::new);

    let mut proxy = RaftStoreProxy::new(
        AtomicU8::new(RaftProxyStatus::Idle as u8),
        encryption_key_manager.clone(),
        Option::None,
        None,
    );

    let proxy_ref = &proxy;
    let proxy_helper = {
        let mut proxy_helper = RaftStoreProxyFFIHelper::new(proxy_ref.into());
        proxy_helper.fn_server_info = Some(ffi_server_info);
        proxy_helper
    };

    info!("set raft-store proxy helper");

    engine_store_server_helper.handle_set_proxy(&proxy_helper);

    info!("wait for engine-store server to start");
    while engine_store_server_helper.handle_get_engine_store_server_status()
        == EngineStoreServerStatus::Idle
    {
        thread::sleep(Duration::from_millis(200));
    }

    if engine_store_server_helper.handle_get_engine_store_server_status()
        != EngineStoreServerStatus::Running
    {
        info!("engine-store server is not running, make proxy exit");
        return;
    }

    info!("engine-store server is started");

    proxy.set_status(RaftProxyStatus::Running);

    {
        debug_assert!(
            engine_store_server_helper.handle_get_engine_store_server_status()
                == EngineStoreServerStatus::Running
        );
        loop {
            if engine_store_server_helper.handle_get_engine_store_server_status()
                != EngineStoreServerStatus::Running
            {
                break;
            }
            thread::sleep(Duration::from_millis(200));
        }
    }

    info!(
        "found engine-store server status is {:?}, start to stop all services",
        engine_store_server_helper.handle_get_engine_store_server_status()
    );

    proxy.set_status(RaftProxyStatus::Stopped);

    info!("all services in raft-store proxy are stopped");

    info!("wait for engine-store server to stop");
    while engine_store_server_helper.handle_get_engine_store_server_status()
        != EngineStoreServerStatus::Terminated
    {
        thread::sleep(Duration::from_millis(200));
    }
    info!("engine-store server is stopped");
}

/// Run a TiKV server. Returns when the server is shutdown by the user, in which
/// case the server will be properly stopped.
pub unsafe fn run_tikv_proxy(
    config: TikvConfig,
    proxy_config: ProxyConfig,
    engine_store_server_helper: &EngineStoreServerHelper,
) {
    // Sets the global logger ASAP.
    // It is okay to use the config w/o `validate()`,
    // because `initial_logger()` handles various conditions.
    initial_logger(&config);

    // Print version information.
    crate::log_proxy_info();

    // Print resource quota.
    SysQuota::log_quota();
    CPU_CORES_QUOTA_GAUGE.set(SysQuota::cpu_cores_quota());

    // Do some prepare works before start.
    pre_start();

    let _m = Monitor::default();

    dispatch_api_version!(config.storage.api_version(), {
        if !config.raft_engine.enable {
            tikv_util::info!("bootstrap with tikv-rocks-engine");
            run_impl::<engine_rocks::RocksEngine, API>(
                config,
                proxy_config,
                engine_store_server_helper,
            )
        } else {
            if proxy_config.engine_store.enable_unips {
                tikv_util::info!("bootstrap with pagestorage");
                run_impl::<PSLogEngine, API>(config, proxy_config, engine_store_server_helper)
            } else {
                tikv_util::info!("bootstrap with tikv-raft-engine");
                run_impl::<RaftLogEngine, API>(config, proxy_config, engine_store_server_helper)
            }
        }
    })
}

/// Run a TiKV server only for decryption. Returns when the server is shutdown
/// by the user, in which case the server will be properly stopped.
pub unsafe fn run_tikv_only_decryption(
    config: TikvConfig,
    proxy_config: ProxyConfig,
    engine_store_server_helper: &EngineStoreServerHelper,
) {
    // Sets the global logger ASAP.
    // It is okay to use the config w/o `validate()`,
    // because `initial_logger()` handles various conditions.
    initial_logger(&config);

    // Print version information.
    crate::log_proxy_info();

    // Print resource quota.
    SysQuota::log_quota();
    CPU_CORES_QUOTA_GAUGE.set(SysQuota::cpu_cores_quota());

    // Do some prepare works before start.
    pre_start();

    let _m = Monitor::default();

    dispatch_api_version!(config.storage.api_version(), {
        if !config.raft_engine.enable {
            run_impl_only_for_decryption::<RocksEngine, API>(
                config,
                proxy_config,
                engine_store_server_helper,
            )
        } else {
            run_impl_only_for_decryption::<RaftLogEngine, API>(
                config,
                proxy_config,
                engine_store_server_helper,
            )
        }
    })
}

impl<CER: ConfiguredRaftEngine> TiKvServer<CER> {
    fn init_tiflash_engines(
        &mut self,
        flow_listener: engine_rocks::FlowListener,
        engine_store_server_helper: isize,
    ) -> (Engines<TiFlashEngine, CER>, Arc<EnginesResourceInfo>) {
        let block_cache = self
            .config
            .storage
            .block_cache
            .build_shared_cache(self.config.storage.engine);
        let env = self
            .config
            .build_shared_rocks_env(self.encryption_key_manager.clone(), get_io_rate_limiter())
            .unwrap();

        // Create raft engine
        let (mut raft_engine, raft_statistics) = CER::build(
            &self.config,
            &env,
            &self.encryption_key_manager,
            &block_cache,
        );
        self.raft_statistics = raft_statistics;

        match raft_engine.as_ps_engine() {
            None => {}
            Some(ps_engine) => {
                ps_engine.init(engine_store_server_helper);
            }
        }

        // Create kv engine.
        let builder = KvEngineFactoryBuilder::new(env, &self.config, block_cache)
            // TODO(tiflash) check if we need a old version of RocksEngine, or if we need to upgrade
            // .compaction_filter_router(self.router.clone())
            .region_info_accessor(self.region_info_accessor.clone())
            .sst_recovery_sender(self.init_sst_recovery_sender())
            .flow_listener(flow_listener);
        let factory = Box::new(builder.build());
        let kv_engine = factory
            .create_shared_db(&self.store_path)
            .unwrap_or_else(|s| fatal!("failed to create kv engine: {}", s));

        self.kv_statistics = Some(factory.rocks_statistics());

        let helper =
            engine_store_ffi::ffi::gen_engine_store_server_helper(engine_store_server_helper);
        let engine_store_hub = Arc::new(engine_store_ffi::engine::TiFlashEngineStoreHub {
            engine_store_server_helper: helper,
        });
        // engine_tiflash::MixedModeEngine has engine_rocks::RocksEngine inside
        let mut kv_engine = TiFlashEngine::from_rocks(kv_engine);
        let proxy_config_set = Arc::new(engine_tiflash::ProxyEngineConfigSet {
            engine_store: self.proxy_config.engine_store.clone(),
        });
        kv_engine.init(
            engine_store_server_helper,
            self.proxy_config.raft_store.snap_handle_pool_size,
            Some(engine_store_hub),
            Some(proxy_config_set),
        );

        let engines = Engines::new(kv_engine.clone(), raft_engine);

        let proxy_rocks_engine = ProxyRocksEngine::new(kv_engine.clone());
        let cfg_controller = self.cfg_controller.as_mut().unwrap();
        cfg_controller.register(
            tikv::config::Module::Rocksdb,
            Box::new(DbConfigManger::new(proxy_rocks_engine, DbType::Kv)),
        );

        let reg = TabletRegistry::new(
            Box::new(SingletonFactory::new(kv_engine.rocks.clone())),
            &self.store_path,
        )
        .unwrap();
        // It always use the singleton kv_engine, use arbitrary id and suffix.
        let ctx = TabletContext::with_infinite_region(0, Some(0));
        reg.load(ctx, false).unwrap();
        self.tablet_registry = Some(reg.clone());
        engines.raft.register_config(cfg_controller);

        let engines_info = Arc::new(EnginesResourceInfo::new(
            &engines, 180, // max_samples_to_preserve
        ));

        (engines, engines_info)
    }
}

const RESERVED_OPEN_FDS: u64 = 1000;

const DEFAULT_METRICS_FLUSH_INTERVAL: Duration = Duration::from_millis(10_000);
const DEFAULT_MEMTRACE_FLUSH_INTERVAL: Duration = Duration::from_millis(1_000);
const DEFAULT_ENGINE_METRICS_RESET_INTERVAL: Duration = Duration::from_millis(60_000);
const DEFAULT_STORAGE_STATS_INTERVAL: Duration = Duration::from_secs(1);

/// A complete TiKV server.
struct TiKvServer<ER: RaftEngine> {
    config: TikvConfig,
    proxy_config: ProxyConfig,
    engine_store_server_helper_ptr: isize,
    cfg_controller: Option<ConfigController>,
    security_mgr: Arc<SecurityManager>,
    pd_client: Arc<RpcClient>,
    router: RaftRouter<TiFlashEngine, ER>,
    flow_info_sender: Option<mpsc::Sender<FlowInfo>>,
    flow_info_receiver: Option<mpsc::Receiver<FlowInfo>>,
    system: Option<RaftBatchSystem<TiFlashEngine, ER>>,
    resolver: Option<resolve::PdStoreAddrResolver>,
    store_path: PathBuf,
    snap_mgr: Option<SnapManager>, // Will be filled in `init_servers`.
    encryption_key_manager: Option<Arc<DataKeyManager>>,
    engines: Option<TiKvEngines<TiFlashEngine, ER>>,
    kv_statistics: Option<Arc<RocksStatistics>>,
    raft_statistics: Option<Arc<RocksStatistics>>,
    servers: Option<Servers<TiFlashEngine, ER>>,
    region_info_accessor: RegionInfoAccessor,
    coprocessor_host: Option<CoprocessorHost<TiFlashEngine>>,
    to_stop: Vec<Box<dyn Stop>>,
    lock_files: Vec<File>,
    concurrency_manager: ConcurrencyManager,
    env: Arc<Environment>,
    background_worker: Worker,
    sst_worker: Option<Box<LazyWorker<String>>>,
    quota_limiter: Arc<QuotaLimiter>,
    resource_manager: Option<Arc<ResourceGroupManager>>,
    tablet_registry: Option<TabletRegistry<RocksEngine>>,
}

struct TiKvEngines<EK: KvEngine, ER: RaftEngine> {
    engines: Engines<EK, ER>,
    store_meta: Arc<Mutex<StoreMeta>>,
    engine: RaftKv<EK, ServerRaftStoreRouter<EK, ER>>,
}

struct Servers<EK: KvEngine, ER: RaftEngine> {
    lock_mgr: LockManager,
    server: LocalServer<EK, ER>,
    node: Node<RpcClient, EK, ER>,
    importer: Arc<SstImporter>,
}

type LocalServer<EK, ER> = Server<resolve::PdStoreAddrResolver, LocalRaftKv<EK, ER>>;
type LocalRaftKv<EK, ER> = RaftKv<EK, ServerRaftStoreRouter<EK, ER>>;

impl<ER: RaftEngine> TiKvServer<ER> {
    fn init(
        mut config: TikvConfig,
        proxy_config: ProxyConfig,
        engine_store_server_helper_ptr: isize,
    ) -> TiKvServer<ER> {
        tikv_util::thread_group::set_properties(Some(GroupProperties::default()));
        // It is okay use pd config and security config before `init_config`,
        // because these configs must be provided by command line, and only
        // used during startup process.
        let security_mgr = Arc::new(
            SecurityManager::new(&config.security)
                .unwrap_or_else(|e| fatal!("failed to create security manager: {}", e)),
        );
        let env = Arc::new(
            EnvBuilder::new()
                .cq_count(config.server.grpc_concurrency)
                .name_prefix(thd_name!(GRPC_THREAD_PREFIX))
                .build(),
        );
        let pd_client =
            Self::connect_to_pd_cluster(&mut config, env.clone(), Arc::clone(&security_mgr));

        // Initialize and check config
        info!("using proxy config"; "config" => ?proxy_config);

        let cfg_controller = Self::init_config(config, &proxy_config);
        let config = cfg_controller.get_current();

        let store_path = Path::new(&config.storage.data_dir).to_owned();

        let thread_count = config.server.background_thread_count;
        let background_worker = WorkerBuilder::new("background")
            .thread_count(thread_count)
            .create();

        let resource_manager = if config.resource_control.enabled {
            let mgr = Arc::new(ResourceGroupManager::default());
            let mut resource_mgr_service =
                ResourceManagerService::new(mgr.clone(), pd_client.clone());
            // spawn a task to periodically update the minimal virtual time of all resource
            // groups.
            let resource_mgr = mgr.clone();
            background_worker.spawn_interval_task(MIN_PRIORITY_UPDATE_INTERVAL, move || {
                resource_mgr.advance_min_virtual_time();
            });
            // spawn a task to watch all resource groups update.
            background_worker.spawn_async_task(async move {
                resource_mgr_service.watch_resource_groups().await;
            });
            Some(mgr)
        } else {
            None
        };
        // Initialize raftstore channels.
        let (router, system) = fsm::create_raft_batch_system(&config.raft_store, &resource_manager);

        let mut coprocessor_host = Some(CoprocessorHost::new(
            router.clone(),
            config.coprocessor.clone(),
        ));
        let region_info_accessor = RegionInfoAccessor::new(coprocessor_host.as_mut().unwrap());

        // Initialize concurrency manager
        let latest_ts = block_on(pd_client.get_tso()).expect("failed to get timestamp from PD");
        let concurrency_manager = ConcurrencyManager::new(latest_ts);

        // use different quota for front-end and back-end requests
        let quota_limiter = Arc::new(QuotaLimiter::new(
            config.quota.foreground_cpu_time,
            config.quota.foreground_write_bandwidth,
            config.quota.foreground_read_bandwidth,
            config.quota.background_cpu_time,
            config.quota.background_write_bandwidth,
            config.quota.background_read_bandwidth,
            config.quota.max_delay_duration,
            config.quota.enable_auto_tune,
        ));

        TiKvServer {
            config,
            proxy_config,
            engine_store_server_helper_ptr,
            cfg_controller: Some(cfg_controller),
            security_mgr,
            pd_client,
            router,
            system: Some(system),
            resolver: None,
            store_path,
            snap_mgr: None,
            encryption_key_manager: None,
            engines: None,
            kv_statistics: None,
            raft_statistics: None,
            servers: None,
            region_info_accessor,
            coprocessor_host,
            to_stop: vec![],
            lock_files: vec![],
            concurrency_manager,
            env,
            background_worker,
            flow_info_sender: None,
            flow_info_receiver: None,
            sst_worker: None,
            quota_limiter,
            resource_manager,
            tablet_registry: None,
        }
    }

    /// Initialize and check the config
    ///
    /// Warnings are logged and fatal errors exist.
    ///
    /// #  Fatal errors
    ///
    /// - If `dynamic config` feature is enabled and failed to register config
    ///   to PD
    /// - If some critical configs (like data dir) are differrent from last run
    /// - If the config can't pass `validate()`
    /// - If the max open file descriptor limit is not high enough to support
    ///   the main database and the raft database.
    fn init_config(mut config: TikvConfig, proxy_config: &ProxyConfig) -> ConfigController {
        crate::config::address_proxy_config(&mut config, proxy_config);
        crate::config::validate_and_persist_config(&mut config, true);
        info!("after address config"; "config" => ?config);

        ensure_dir_exist(&config.storage.data_dir).unwrap();
        if !config.rocksdb.wal_dir.is_empty() {
            ensure_dir_exist(&config.rocksdb.wal_dir).unwrap();
        }
        if config.raft_engine.enable {
            ensure_dir_exist(&config.raft_engine.config().dir).unwrap();
        } else {
            ensure_dir_exist(&config.raft_store.raftdb_path).unwrap();
            if !config.raftdb.wal_dir.is_empty() {
                ensure_dir_exist(&config.raftdb.wal_dir).unwrap();
            }
        }

        check_system_config(&config);

        tikv_util::set_panic_hook(config.abort_on_panic, &config.storage.data_dir);

        info!(
            "using config";
            "config" => serde_json::to_string(&config).unwrap(),
        );
        if config.panic_when_unexpected_key_or_data {
            info!("panic-when-unexpected-key-or-data is on");
            tikv_util::set_panic_when_unexpected_key_or_data(true);
        }

        config.write_into_metrics();

        ConfigController::new(config)
    }

    fn connect_to_pd_cluster(
        config: &mut TikvConfig,
        env: Arc<Environment>,
        security_mgr: Arc<SecurityManager>,
    ) -> Arc<RpcClient> {
        let pd_client = Arc::new(
            RpcClient::new(&config.pd, Some(env), security_mgr)
                .unwrap_or_else(|e| fatal!("failed to create rpc client: {}", e)),
        );

        let cluster_id = pd_client
            .get_cluster_id()
            .unwrap_or_else(|e| fatal!("failed to get cluster id: {}", e));
        if cluster_id == DEFAULT_CLUSTER_ID {
            fatal!("cluster id can't be {}", DEFAULT_CLUSTER_ID);
        }
        config.server.cluster_id = cluster_id;
        info!(
            "connect to PD cluster";
            "cluster_id" => cluster_id
        );

        pd_client
    }

    fn check_conflict_addr(&mut self) {
        let cur_addr: SocketAddr = self
            .config
            .server
            .addr
            .parse()
            .expect("failed to parse into a socket address");
        let cur_ip = cur_addr.ip();
        let cur_port = cur_addr.port();
        let lock_dir = get_lock_dir();

        let search_base = env::temp_dir().join(&lock_dir);
        file_system::create_dir_all(&search_base)
            .unwrap_or_else(|_| panic!("create {} failed", search_base.display()));

        for entry in file_system::read_dir(&search_base).unwrap().flatten() {
            if !entry.file_type().unwrap().is_file() {
                continue;
            }
            let file_path = entry.path();
            let file_name = file_path.file_name().unwrap().to_str().unwrap();
            if let Ok(addr) = file_name.replace('_', ":").parse::<SocketAddr>() {
                let ip = addr.ip();
                let port = addr.port();
                if cur_port == port
                    && (cur_ip == ip || cur_ip.is_unspecified() || ip.is_unspecified())
                {
                    let _ = try_lock_conflict_addr(file_path);
                }
            }
        }

        let cur_path = search_base.join(cur_addr.to_string().replace(':', "_"));
        let cur_file = try_lock_conflict_addr(cur_path);
        self.lock_files.push(cur_file);
    }

    fn init_fs(&mut self) {
        let lock_path = self.store_path.join(Path::new("LOCK"));

        let f = File::create(lock_path.as_path())
            .unwrap_or_else(|e| fatal!("failed to create lock at {}: {}", lock_path.display(), e));
        if f.try_lock_exclusive().is_err() {
            fatal!(
                "lock {} failed, maybe another instance is using this directory.",
                self.store_path.display()
            );
        }
        self.lock_files.push(f);

        if tikv_util::panic_mark_file_exists(&self.config.storage.data_dir) {
            fatal!(
                "panic_mark_file {} exists, there must be something wrong with the db. \
                     Do not remove the panic_mark_file and force the TiKV node to restart. \
                     Please contact TiKV maintainers to investigate the issue. \
                     If needed, use scale in and scale out to replace the TiKV node. \
                     https://docs.pingcap.com/tidb/stable/scale-tidb-using-tiup",
                tikv_util::panic_mark_file_path(&self.config.storage.data_dir).display()
            );
        }

        // We truncate a big file to make sure that both raftdb and kvdb of TiKV have
        // enough space to do compaction and region migration when TiKV recover.
        // This file is created in data_dir rather than db_path, because we must
        // not increase store size of db_path.
        let disk_stats = fs2::statvfs(&self.config.storage.data_dir).unwrap();
        let mut capacity = disk_stats.total_space();
        if self.config.raft_store.capacity.0 > 0 {
            capacity = cmp::min(capacity, self.config.raft_store.capacity.0);
        }

        let mut reserve_space = self.config.storage.reserve_space.0;
        if self.config.storage.reserve_space.0 != 0 {
            reserve_space = cmp::max(
                (capacity as f64 * 0.05) as u64,
                self.config.storage.reserve_space.0,
            );
        }
        disk::set_disk_reserved_space(reserve_space);
        let path =
            Path::new(&self.config.storage.data_dir).join(file_system::SPACE_PLACEHOLDER_FILE);
        if let Err(e) = file_system::remove_file(&path) {
            warn!("failed to remove space holder on starting: {}", e);
        }

        let available = disk_stats.available_space();
        // place holder file size is 20% of total reserved space.
        if available > reserve_space {
            file_system::reserve_space_for_recover(
                &self.config.storage.data_dir,
                reserve_space / 5,
            )
            .map_err(|e| panic!("Failed to reserve space for recovery: {}.", e))
            .unwrap();
        } else {
            warn!("no enough disk space left to create the place holder file");
        }
    }

    fn init_yatp(&self) {
        yatp::metrics::set_namespace(Some("tikv"));
        prometheus::register(Box::new(yatp::metrics::MULTILEVEL_LEVEL0_CHANCE.clone())).unwrap();
        prometheus::register(Box::new(yatp::metrics::MULTILEVEL_LEVEL_ELAPSED.clone())).unwrap();
    }

    fn init_encryption(&mut self) {
        self.encryption_key_manager = data_key_manager_from_config(
            &self.config.security.encryption,
            &self.config.storage.data_dir,
        )
        .map_err(|e| {
            panic!(
                "Encryption failed to initialize: {}. code: {}",
                e,
                e.error_code()
            )
        })
        .unwrap()
        .map(Arc::new);
    }

    fn init_flow_receiver(&mut self) -> engine_rocks::FlowListener {
        let (tx, rx) = mpsc::channel();
        self.flow_info_sender = Some(tx.clone());
        self.flow_info_receiver = Some(rx);
        engine_rocks::FlowListener::new(tx)
    }

    pub fn init_engines(&mut self, engines: Engines<TiFlashEngine, ER>) {
        let store_meta = Arc::new(Mutex::new(StoreMeta::new(PENDING_MSG_CAP)));
        let engine = RaftKv::new(
            ServerRaftStoreRouter::new(
                self.router.clone(),
                LocalReader::new(
                    engines.kv.clone(),
                    StoreMetaDelegate::new(store_meta.clone(), engines.kv.clone()),
                    self.router.clone(),
                ),
            ),
            engines.kv.clone(),
            self.region_info_accessor.region_leaders(),
        );

        self.engines = Some(TiKvEngines {
            engines,
            store_meta,
            engine,
        });
    }

    fn init_gc_worker(
        &mut self,
    ) -> GcWorker<RaftKv<TiFlashEngine, ServerRaftStoreRouter<TiFlashEngine, ER>>> {
        let engines = self.engines.as_ref().unwrap();
        let gc_worker = GcWorker::new(
            engines.engine.clone(),
            self.flow_info_sender.take().unwrap(),
            self.config.gc.clone(),
            self.pd_client.feature_gate().clone(),
            Arc::new(self.region_info_accessor.clone()),
        );

        let cfg_controller = self.cfg_controller.as_mut().unwrap();
        cfg_controller.register(
            tikv::config::Module::Gc,
            Box::new(gc_worker.get_config_manager()),
        );

        gc_worker
    }

    fn init_servers<F: KvFormat>(&mut self) -> Arc<VersionTrack<ServerConfig>> {
        let flow_controller = Arc::new(FlowController::Singleton(EngineFlowController::new(
            &self.config.storage.flow_control,
            self.engines.as_ref().unwrap().engine.kv_engine().unwrap(),
            self.flow_info_receiver.take().unwrap(),
        )));
        let mut gc_worker = self.init_gc_worker();
        let mut ttl_checker = Box::new(LazyWorker::new("ttl-checker"));
        let ttl_scheduler = ttl_checker.scheduler();

        let cfg_controller = self.cfg_controller.as_mut().unwrap();

        cfg_controller.register(
            tikv::config::Module::Quota,
            Box::new(QuotaLimitConfigManager::new(Arc::clone(
                &self.quota_limiter,
            ))),
        );

        // Create cdc.
        // let mut cdc_worker = Box::new(LazyWorker::new("cdc"));
        // let cdc_scheduler = cdc_worker.scheduler();
        // let txn_extra_scheduler =
        // cdc::CdcTxnExtraScheduler::new(cdc_scheduler.clone());
        //
        // self.engines
        //     .as_mut()
        //     .unwrap()
        //     .engine
        //     .set_txn_extra_scheduler(Arc::new(txn_extra_scheduler));

        // let lock_mgr = LockManager::new(&self.config.pessimistic_txn);
        let lock_mgr = LockManager::new();
        // cfg_controller.register(
        //     tikv::config::Module::PessimisticTxn,
        //     Box::new(lock_mgr.config_manager()),
        // );
        // lock_mgr.register_detector_role_change_observer(self.coprocessor_host.
        // as_mut().unwrap());

        let engines = self.engines.as_ref().unwrap();

        let pd_worker = LazyWorker::new("pd-worker");
        let pd_sender = pd_worker.scheduler();

        if let Some(sst_worker) = &mut self.sst_worker {
            let sst_runner = RecoveryRunner::new(
                engines.engines.kv.rocks.clone(),
                engines.store_meta.clone(),
                self.config.storage.background_error_recovery_window.into(),
                DEFAULT_CHECK_INTERVAL,
            );
            sst_worker.start_with_timer(sst_runner);
        }

        let unified_read_pool = if self.config.readpool.is_unified_pool_enabled() {
            let resource_ctl = self
                .resource_manager
                .as_ref()
                .map(|m| m.derive_controller("unified-read-pool".into(), true));

            Some(build_yatp_read_pool(
                &self.config.readpool.unified,
                pd_sender.clone(),
                engines.engine.clone(),
                resource_ctl,
                CleanupMethod::Remote(self.background_worker.remote()),
            ))
        } else {
            None
        };

        // The `DebugService` and `DiagnosticsService` will share the same thread pool
        let props = tikv_util::thread_group::current_properties();
        let debug_thread_pool = Arc::new(
            Builder::new_multi_thread()
                .thread_name(thd_name!("debugger"))
                .worker_threads(1)
                .after_start_wrapper(move || {
                    tikv_alloc::add_thread_memory_accessor();
                    tikv_util::thread_group::set_properties(props.clone());
                })
                .before_stop_wrapper(tikv_alloc::remove_thread_memory_accessor)
                .build()
                .unwrap(),
        );

        // TODO(tiflash) Maybe we can remove this service.
        // Start resource metering.
        let (recorder_notifier, collector_reg_handle, resource_tag_factory, recorder_worker) =
            resource_metering::init_recorder(self.config.resource_metering.precision.as_millis());
        self.to_stop.push(recorder_worker);
        let (reporter_notifier, data_sink_reg_handle, reporter_worker) =
            resource_metering::init_reporter(
                self.config.resource_metering.clone(),
                collector_reg_handle.clone(),
            );
        self.to_stop.push(reporter_worker);
        let (address_change_notifier, single_target_worker) = resource_metering::init_single_target(
            self.config.resource_metering.receiver_address.clone(),
            self.env.clone(),
            data_sink_reg_handle.clone(),
        );
        self.to_stop.push(single_target_worker);

        let cfg_manager = resource_metering::ConfigManager::new(
            self.config.resource_metering.clone(),
            recorder_notifier,
            reporter_notifier,
            address_change_notifier,
        );
        cfg_controller.register(
            tikv::config::Module::ResourceMetering,
            Box::new(cfg_manager),
        );

        let storage_read_pool_handle = if self.config.readpool.storage.use_unified_pool() {
            unified_read_pool.as_ref().unwrap().handle()
        } else {
            let storage_read_pools = ReadPool::from(storage::build_read_pool(
                &self.config.readpool.storage,
                pd_sender.clone(),
                engines.engine.clone(),
            ));
            storage_read_pools.handle()
        };

        // we don't care since we don't start this service
        let dummy_dynamic_configs = tikv::storage::DynamicConfigs {
            pipelined_pessimistic_lock: Arc::new(AtomicBool::new(true)),
            in_memory_pessimistic_lock: Arc::new(AtomicBool::new(true)),
            wake_up_delay_duration_ms: Arc::new(AtomicU64::new(
                ReadableDuration::millis(20).as_millis(),
            )),
        };

        let storage = Storage::<_, _, F>::from_engine(
            engines.engine.clone(),
            &self.config.storage,
            storage_read_pool_handle,
            lock_mgr.clone(),
            self.concurrency_manager.clone(),
            dummy_dynamic_configs,
            flow_controller.clone(),
            pd_sender.clone(),
            resource_tag_factory.clone(),
            Arc::clone(&self.quota_limiter),
            self.pd_client.feature_gate().clone(),
            None, // causal_ts_provider
            self.resource_manager
                .as_ref()
                .map(|m| m.derive_controller("scheduler-worker-pool".to_owned(), true)),
        )
        .unwrap_or_else(|e| fatal!("failed to create raft storage: {}", e));

        cfg_controller.register(
            tikv::config::Module::Storage,
            Box::new(StorageConfigManger::new(
                self.tablet_registry.as_ref().unwrap().clone(),
                ttl_scheduler,
                flow_controller,
                storage.get_scheduler(),
            )),
        );

        let (resolver, state) = resolve::new_resolver(
            self.pd_client.clone(),
            &self.background_worker,
            storage.get_engine().raft_extension().clone(),
        );
        self.resolver = Some(resolver);

        ReplicaReadLockChecker::new(self.concurrency_manager.clone())
            .register(self.coprocessor_host.as_mut().unwrap());

        // Create snapshot manager, server.
        let snap_path = self
            .store_path
            .join(Path::new("snap"))
            .to_str()
            .unwrap()
            .to_owned();

        let bps = i64::try_from(self.config.server.snap_max_write_bytes_per_sec.0)
            .unwrap_or_else(|_| fatal!("snap_max_write_bytes_per_sec > i64::max_value"));

        let snap_mgr = SnapManagerBuilder::default()
            .max_write_bytes_per_sec(bps)
            .max_total_size(self.config.server.snap_max_total_size.0)
            .encryption_key_manager(self.encryption_key_manager.clone())
            .max_per_file_size(self.config.raft_store.max_snapshot_file_raw_size.0)
            .enable_multi_snapshot_files(
                self.pd_client
                    .feature_gate()
                    .can_enable(MULTI_FILES_SNAPSHOT_FEATURE),
            )
            .build(snap_path);

        // Create coprocessor endpoint.
        let cop_read_pool_handle = if self.config.readpool.coprocessor.use_unified_pool() {
            unified_read_pool.as_ref().unwrap().handle()
        } else {
            let cop_read_pools = ReadPool::from(coprocessor::readpool_impl::build_read_pool(
                &self.config.readpool.coprocessor,
                pd_sender,
                engines.engine.clone(),
            ));
            cop_read_pools.handle()
        };

        let mut unified_read_pool_scale_receiver = None;
        if self.config.readpool.is_unified_pool_enabled() {
            let (unified_read_pool_scale_notifier, rx) = mpsc::sync_channel(10);
            cfg_controller.register(
                tikv::config::Module::Readpool,
                Box::new(ReadPoolConfigManager::new(
                    unified_read_pool.as_ref().unwrap().handle(),
                    unified_read_pool_scale_notifier,
                    &self.background_worker,
                    self.config.readpool.unified.max_thread_count,
                    self.config.readpool.unified.auto_adjust_pool_size,
                )),
            );
            unified_read_pool_scale_receiver = Some(rx);
        }

        // // Register causal observer for RawKV API V2
        // if let ApiVersion::V2 = F::TAG {
        //     let tso = block_on(causal_ts::BatchTsoProvider::new_opt(
        //         self.pd_client.clone(),
        //         self.config.causal_ts.renew_interval.0,
        //         self.config.causal_ts.renew_batch_min_size,
        //     ));
        //     if let Err(e) = tso {
        //         panic!("Causal timestamp provider initialize failed: {:?}", e);
        //     }
        //     let causal_ts_provider = Arc::new(tso.unwrap());
        //     info!("Causal timestamp provider startup.");
        //
        //     let causal_ob = causal_ts::CausalObserver::new(causal_ts_provider);
        //     causal_ob.register_to(self.coprocessor_host.as_mut().unwrap());
        // }

        // // Register cdc.
        // let cdc_ob = cdc::CdcObserver::new(cdc_scheduler.clone());
        // cdc_ob.register_to(self.coprocessor_host.as_mut().unwrap());
        // // Register cdc config manager.
        // cfg_controller.register(
        //     tikv::config::Module::CDC,
        //     Box::new(CdcConfigManager(cdc_worker.scheduler())),
        // );

        // // Create resolved ts worker
        // let rts_worker = if self.config.resolved_ts.enable {
        //     let worker = Box::new(LazyWorker::new("resolved-ts"));
        //     // Register the resolved ts observer
        //     let resolved_ts_ob = resolved_ts::Observer::new(worker.scheduler());
        //     resolved_ts_ob.register_to(self.coprocessor_host.as_mut().unwrap());
        //     // Register config manager for resolved ts worker
        //     cfg_controller.register(
        //         tikv::config::Module::ResolvedTs,
        //         Box::new(resolved_ts::ResolvedTsConfigManager::new(
        //             worker.scheduler(),
        //         )),
        //     );
        //     Some(worker)
        // } else {
        //     None
        // };

        let server_config = Arc::new(VersionTrack::new(self.config.server.clone()));

        self.config
            .raft_store
            .validate(
                self.config.coprocessor.region_split_size(),
                self.config.coprocessor.enable_region_bucket(),
                self.config.coprocessor.region_bucket_size,
            )
            .unwrap_or_else(|e| fatal!("failed to validate raftstore config {}", e));
        let raft_store = Arc::new(VersionTrack::new(self.config.raft_store.clone()));
        let health_service = HealthService::default();
        let mut default_store = kvproto::metapb::Store::default();

        if !self.proxy_config.server.engine_store_version.is_empty() {
            default_store.set_version(self.proxy_config.server.engine_store_version.clone());
        }
        if !self.proxy_config.server.engine_store_git_hash.is_empty() {
            default_store.set_git_hash(self.proxy_config.server.engine_store_git_hash.clone());
        }
        // addr -> store.peer_address
        if self.config.server.advertise_addr.is_empty() {
            default_store.set_peer_address(self.config.server.addr.clone());
        } else {
            default_store.set_peer_address(self.config.server.advertise_addr.clone())
        }
        // engine_addr -> store.addr
        if !self.proxy_config.server.engine_addr.is_empty() {
            default_store.set_address(self.proxy_config.server.engine_addr.clone());
        } else {
            panic!("engine address is empty");
        }

        let mut node = Node::new(
            self.system.take().unwrap(),
            &server_config.value().clone(),
            raft_store.clone(),
            self.config.storage.api_version(),
            self.pd_client.clone(),
            state,
            self.background_worker.clone(),
            Some(health_service.clone()),
            Some(default_store),
        );
        node.try_bootstrap_store(engines.engines.clone())
            .unwrap_or_else(|e| fatal!("failed to bootstrap node id: {}", e));

        {
            engine_store_ffi::ffi::gen_engine_store_server_helper(
                self.engine_store_server_helper_ptr,
            )
            .set_store(node.store());
            info!("set store {} to engine-store", node.id());
        }

        let import_path = self.store_path.join("import");
        let mut importer = SstImporter::new(
            &self.config.import,
            import_path,
            self.encryption_key_manager.clone(),
            self.config.storage.api_version(),
        )
        .unwrap();
        for (cf_name, compression_type) in &[
            (
                CF_DEFAULT,
                self.config.rocksdb.defaultcf.bottommost_level_compression,
            ),
            (
                CF_WRITE,
                self.config.rocksdb.writecf.bottommost_level_compression,
            ),
        ] {
            importer.set_compression_type(cf_name, from_rocks_compression_type(*compression_type));
        }
        let importer = Arc::new(importer);

        let check_leader_runner = CheckLeaderRunner::new(
            engines.store_meta.clone(),
            self.coprocessor_host.clone().unwrap(),
        );
        let check_leader_scheduler = self
            .background_worker
            .start("check-leader", check_leader_runner);

        self.snap_mgr = Some(snap_mgr.clone());
        // Create server
        let server = Server::new(
            node.id(),
            &server_config,
            &self.security_mgr,
            storage,
            coprocessor::Endpoint::new(
                &server_config.value(),
                cop_read_pool_handle,
                self.concurrency_manager.clone(),
                resource_tag_factory,
                Arc::clone(&self.quota_limiter),
            ),
            coprocessor_v2::Endpoint::new(&self.config.coprocessor_v2),
            self.resolver.clone().unwrap(),
            Either::Left(snap_mgr.clone()),
            gc_worker.clone(),
            check_leader_scheduler,
            self.env.clone(),
            unified_read_pool,
            debug_thread_pool,
            health_service,
        )
        .unwrap_or_else(|e| fatal!("failed to create server: {}", e));

        let packed_envs = engine_store_ffi::core::PackedEnvs {
            engine_store_cfg: self.proxy_config.engine_store.clone(),
            pd_endpoints: self.config.pd.endpoints.clone(),
            snap_handle_pool_size: self.proxy_config.raft_store.snap_handle_pool_size,
        };
        let tiflash_ob = engine_store_ffi::observer::TiFlashObserver::new(
            node.id(),
            self.engines.as_ref().unwrap().engines.kv.clone(),
            self.engines.as_ref().unwrap().engines.raft.clone(),
            importer.clone(),
            server.transport().clone(),
            snap_mgr.clone(),
            packed_envs,
            DebugStruct::default(),
        );
        tiflash_ob.register_to(self.coprocessor_host.as_mut().unwrap());

        cfg_controller.register(
            tikv::config::Module::Server,
            Box::new(ServerConfigManager::new(
                server.get_snap_worker_scheduler(),
                server_config.clone(),
                server.get_grpc_mem_quota().clone(),
            )),
        );

        // // Start backup stream
        // if self.config.backup_stream.enable {
        //     // Create backup stream.
        //     let mut backup_stream_worker =
        // Box::new(LazyWorker::new("backup-stream"));
        //     let backup_stream_scheduler = backup_stream_worker.scheduler();
        //
        //     // Register backup-stream observer.
        //     let backup_stream_ob =
        // BackupStreamObserver::new(backup_stream_scheduler.clone());
        //     backup_stream_ob.register_to(self.coprocessor_host.as_mut().unwrap());
        //     // Register config manager.
        //     cfg_controller.register(
        //         tikv::config::Module::BackupStream,
        //         Box::new(BackupStreamConfigManager(backup_stream_worker.
        // scheduler())),     );
        //
        //     let backup_stream_endpoint = backup_stream::Endpoint::new::<String>(
        //         node.id(),
        //         &self.config.pd.endpoints,
        //         self.config.backup_stream.clone(),
        //         backup_stream_scheduler,
        //         backup_stream_ob,
        //         self.region_info_accessor.clone(),
        //         self.router.clone(),
        //         self.pd_client.clone(),
        //         self.concurrency_manager.clone(),
        //     );
        //     backup_stream_worker.start(backup_stream_endpoint);
        //     self.to_stop.push(backup_stream_worker);
        // }

        let split_check_runner = SplitCheckRunner::new(
            engines.engines.kv.clone(),
            self.router.clone(),
            self.coprocessor_host.clone().unwrap(),
        );
        let split_check_scheduler = self
            .background_worker
            .start("split-check", split_check_runner);
        cfg_controller.register(
            tikv::config::Module::Coprocessor,
            Box::new(SplitCheckConfigManager(split_check_scheduler.clone())),
        );

        let split_config_manager =
            SplitConfigManager::new(Arc::new(VersionTrack::new(self.config.split.clone())));
        cfg_controller.register(
            tikv::config::Module::Split,
            Box::new(split_config_manager.clone()),
        );

        let auto_split_controller = AutoSplitController::new(
            split_config_manager,
            self.config.server.grpc_concurrency,
            self.config.readpool.unified.max_thread_count,
            unified_read_pool_scale_receiver,
        );

        node.start(
            engines.engines.clone(),
            server.transport(),
            snap_mgr,
            pd_worker,
            engines.store_meta.clone(),
            self.coprocessor_host.clone().unwrap(),
            importer.clone(),
            split_check_scheduler,
            auto_split_controller,
            self.concurrency_manager.clone(),
            collector_reg_handle,
            None,
        )
        .unwrap_or_else(|e| fatal!("failed to start node: {}", e));

        gc_worker
            .start(node.id())
            .unwrap_or_else(|e| fatal!("failed to start gc worker: {}", e));

        initial_metric(&self.config.metric);
        if self.config.storage.enable_ttl {
            ttl_checker.start_with_timer(TtlChecker::new(
                self.engines.as_ref().unwrap().engine.kv_engine().unwrap(),
                self.region_info_accessor.clone(),
                self.config.storage.ttl_check_poll_interval.into(),
            ));
            self.to_stop.push(ttl_checker);
        }

        // Start CDC.
        // Start resolved ts

        cfg_controller.register(
            tikv::config::Module::Raftstore,
            Box::new(RaftstoreConfigManager::new(
                node.refresh_config_scheduler(),
                raft_store,
            )),
        );

        self.servers = Some(Servers {
            lock_mgr,
            server,
            node,
            importer,
        });

        server_config
    }

    fn register_services(&mut self) {
        let servers = self.servers.as_mut().unwrap();
        let engines = self.engines.as_ref().unwrap();

        // Import SST service.
        let import_service = ImportSstService::new(
            self.config.import.clone(),
            self.config.raft_store.raft_entry_max_size,
            self.router.clone(),
            engines.engines.kv.clone(),
            servers.importer.clone(),
        );
        if servers
            .server
            .register_service(create_import_sst(import_service))
            .is_some()
        {
            fatal!("failed to register import service");
        }

        // Debug service.
        let debug_service = DebugService::new(
            Engines {
                kv: engines.engines.kv.rocks.clone(),
                raft: engines.engines.raft.clone(),
            },
            self.kv_statistics.clone(),
            self.raft_statistics.clone(),
            servers.server.get_debug_thread_pool().clone(),
            engines.engine.raft_extension().clone(),
            self.cfg_controller.as_ref().unwrap().clone(),
        );
        if servers
            .server
            .register_service(create_debug(debug_service))
            .is_some()
        {
            fatal!("failed to register debug service");
        }

        // Create Diagnostics service
        let diag_service = DiagnosticsService::new(
            servers.server.get_debug_thread_pool().clone(),
            self.config.log.file.filename.clone(),
            self.config.slow_log_file.clone(),
        );
        if servers
            .server
            .register_service(create_diagnostics(diag_service))
            .is_some()
        {
            fatal!("failed to register diagnostics service");
        }

        // Lock manager.
        // Backup service.
    }

    fn init_io_utility(&mut self) -> BytesFetcher {
        let stats_collector_enabled = file_system::init_io_stats_collector()
            .map_err(|e| warn!("failed to init I/O stats collector: {}", e))
            .is_ok();

        let limiter = Arc::new(
            self.config
                .storage
                .io_rate_limit
                .build(!stats_collector_enabled /* enable_statistics */),
        );
        let fetcher = if stats_collector_enabled {
            BytesFetcher::FromIoStatsCollector()
        } else {
            BytesFetcher::FromRateLimiter(limiter.statistics().unwrap())
        };
        // Set up IO limiter even when rate limit is disabled, so that rate limits can
        // be dynamically applied later on.
        set_io_rate_limiter(Some(limiter));
        fetcher
    }

    fn init_metrics_flusher(
        &mut self,
        fetcher: BytesFetcher,
        engines_info: Arc<EnginesResourceInfo>,
    ) {
        let mut engine_metrics = EngineMetricsManager::<RocksEngine, ER>::new(
            self.tablet_registry.clone().unwrap(),
            self.kv_statistics.clone(),
            self.config.rocksdb.titan.enabled,
            self.engines.as_ref().unwrap().engines.raft.clone(),
            self.raft_statistics.clone(),
        );
        let mut io_metrics = IOMetricsManager::new(fetcher);
        let engines_info_clone = engines_info.clone();
        self.background_worker
            .spawn_interval_task(DEFAULT_METRICS_FLUSH_INTERVAL, move || {
                let now = Instant::now();
                engine_metrics.flush(now);
                io_metrics.flush(now);
                engines_info_clone.update(now);
            });
        if let Some(limiter) = get_io_rate_limiter() {
            limiter.set_low_priority_io_adjustor_if_needed(Some(engines_info));
        }

        let mut mem_trace_metrics = MemoryTraceManager::default();
        mem_trace_metrics.register_provider(MEMTRACE_RAFTSTORE.clone());
        mem_trace_metrics.register_provider(MEMTRACE_COPROCESSOR.clone());
        self.background_worker
            .spawn_interval_task(DEFAULT_MEMTRACE_FLUSH_INTERVAL, move || {
                let now = Instant::now();
                mem_trace_metrics.flush(now);
            });
    }

    fn init_storage_stats_task(&self, engines: Engines<TiFlashEngine, ER>) {
        let config_disk_capacity: u64 = self.config.raft_store.capacity.0;
        let data_dir = self.config.storage.data_dir.clone();
        let store_path = self.store_path.clone();
        let snap_mgr = self.snap_mgr.clone().unwrap();
        let reserve_space = disk::get_disk_reserved_space();
        if reserve_space == 0 {
            info!("disk space checker not enabled");
            return;
        }

        let almost_full_threshold = reserve_space;
        let already_full_threshold = reserve_space / 2;
        self.background_worker
            .spawn_interval_task(DEFAULT_STORAGE_STATS_INTERVAL, move || {
                let disk_stats = match fs2::statvfs(&store_path) {
                    Err(e) => {
                        error!(
                            "get disk stat for kv store failed";
                            "kv path" => store_path.to_str(),
                            "err" => ?e
                        );
                        return;
                    }
                    Ok(stats) => stats,
                };
                let disk_cap = disk_stats.total_space();
                let snap_size = snap_mgr.get_total_snap_size().unwrap();

                let kv_size = engines
                    .kv
                    .get_engine_used_size()
                    .expect("get kv engine size");

                let raft_size = engines
                    .raft
                    .get_engine_size()
                    .expect("get raft engine size");

                let placeholer_file_path = PathBuf::from_str(&data_dir)
                    .unwrap()
                    .join(Path::new(file_system::SPACE_PLACEHOLDER_FILE));

                let placeholder_size: u64 =
                    file_system::get_file_size(&placeholer_file_path).unwrap_or(0);

                let used_size = snap_size + kv_size + raft_size + placeholder_size;
                let capacity = if config_disk_capacity == 0 || disk_cap < config_disk_capacity {
                    disk_cap
                } else {
                    config_disk_capacity
                };

                let mut available = capacity.checked_sub(used_size).unwrap_or_default();
                available = cmp::min(available, disk_stats.available_space());

                let prev_disk_status = disk::get_disk_status(0); //0 no need care about failpoint.
                let cur_disk_status = if available <= already_full_threshold {
                    disk::DiskUsage::AlreadyFull
                } else if available <= almost_full_threshold {
                    disk::DiskUsage::AlmostFull
                } else {
                    disk::DiskUsage::Normal
                };
                if prev_disk_status != cur_disk_status {
                    warn!(
                        "disk usage {:?}->{:?}, available={},snap={},kv={},raft={},capacity={}",
                        prev_disk_status,
                        cur_disk_status,
                        available,
                        snap_size,
                        kv_size,
                        raft_size,
                        capacity
                    );
                }
                disk::set_disk_status(cur_disk_status);
            })
    }

    fn init_sst_recovery_sender(&mut self) -> Option<Scheduler<String>> {
        if !self
            .config
            .storage
            .background_error_recovery_window
            .is_zero()
        {
            let sst_worker = Box::new(LazyWorker::new("sst-recovery"));
            let scheduler = sst_worker.scheduler();
            self.sst_worker = Some(sst_worker);
            Some(scheduler)
        } else {
            None
        }
    }

    fn run_server(&mut self, server_config: Arc<VersionTrack<ServerConfig>>) {
        let server = self.servers.as_mut().unwrap();
        server
            .server
            .build_and_bind()
            .unwrap_or_else(|e| fatal!("failed to build server: {}", e));
        server
            .server
            .start(server_config, self.security_mgr.clone())
            .unwrap_or_else(|e| fatal!("failed to start server: {}", e));
    }

    fn run_status_server(&mut self) {
        // Create a status server.
        let status_enabled = !self.config.server.status_addr.is_empty();
        if status_enabled {
            let mut status_server = match StatusServer::new(
                engine_store_ffi::ffi::gen_engine_store_server_helper(
                    self.engine_store_server_helper_ptr,
                ),
                self.config.server.status_thread_pool_size,
                self.cfg_controller.take().unwrap(),
                Arc::new(self.config.security.clone()),
                self.router.clone(),
                self.store_path.clone(),
            ) {
                Ok(status_server) => Box::new(status_server),
                Err(e) => {
                    error_unknown!(%e; "failed to start runtime for status service");
                    return;
                }
            };
            // Start the status server.
            if let Err(e) = status_server.start(self.config.server.status_addr.clone()) {
                error_unknown!(%e; "failed to bind addr for status service");
            } else {
                self.to_stop.push(status_server);
            }
        }
    }

    fn stop(self) {
        tikv_util::thread_group::mark_shutdown();
        let mut servers = self.servers.unwrap();
        servers
            .server
            .stop()
            .unwrap_or_else(|e| fatal!("failed to stop server: {}", e));

        servers.node.stop();
        self.region_info_accessor.stop();

        servers.lock_mgr.stop();

        if let Some(sst_worker) = self.sst_worker {
            sst_worker.stop_worker();
        }

        self.to_stop.into_iter().for_each(|s| s.stop());
    }
}

pub trait ConfiguredRaftEngine: RaftEngine {
    fn build(
        _: &TikvConfig,
        _: &Arc<Env>,
        _: &Option<Arc<DataKeyManager>>,
        _: &Cache,
    ) -> (Self, Option<Arc<RocksStatistics>>);
    fn as_rocks_engine(&self) -> Option<&RocksEngine> {
        None
    }

    fn register_config(&self, _cfg_controller: &mut ConfigController) {}

    fn as_ps_engine(&mut self) -> Option<&mut PSLogEngine> {
        None
    }
}

impl ConfiguredRaftEngine for engine_rocks::RocksEngine {
    fn build(
        config: &TikvConfig,
        env: &Arc<Env>,
        key_manager: &Option<Arc<DataKeyManager>>,
        block_cache: &Cache,
    ) -> (Self, Option<Arc<RocksStatistics>>) {
        let mut raft_data_state_machine = RaftDataStateMachine::new(
            &config.storage.data_dir,
            &config.raft_engine.config().dir,
            &config.raft_store.raftdb_path,
        );
        let should_dump = raft_data_state_machine.before_open_target();

        let raft_db_path = &config.raft_store.raftdb_path;
        let config_raftdb = &config.raftdb;
        let statistics = Arc::new(RocksStatistics::new_titan());
        let raft_db_opts = config_raftdb.build_opt(env.clone(), Some(&statistics));
        let raft_cf_opts = config_raftdb.build_cf_opts(block_cache);
        let raftdb = engine_rocks::util::new_engine_opt(raft_db_path, raft_db_opts, raft_cf_opts)
            .expect("failed to open raftdb");

        if should_dump {
            let raft_engine =
                RaftLogEngine::new(config.raft_engine.config(), key_manager.clone(), None)
                    .expect("failed to open raft engine for migration");
            dump_raft_engine_to_raftdb(&raft_engine, &raftdb, 8 /* threads */);
            raft_engine.stop();
            drop(raft_engine);
            raft_data_state_machine.after_dump_data();
        }
        (raftdb, Some(statistics))
    }

    fn as_rocks_engine(&self) -> Option<&RocksEngine> {
        Some(self)
    }

    fn register_config(&self, cfg_controller: &mut ConfigController) {
        cfg_controller.register(
            tikv::config::Module::Raftdb,
            Box::new(DbConfigManger::new(self.clone(), DbType::Raft)),
        );
    }
}

impl ConfiguredRaftEngine for RaftLogEngine {
    fn build(
        config: &TikvConfig,
        env: &Arc<Env>,
        key_manager: &Option<Arc<DataKeyManager>>,
        block_cache: &Cache,
    ) -> (Self, Option<Arc<RocksStatistics>>) {
        let mut raft_data_state_machine = RaftDataStateMachine::new(
            &config.storage.data_dir,
            &config.raft_store.raftdb_path,
            &config.raft_engine.config().dir,
        );
        let should_dump = raft_data_state_machine.before_open_target();

        let raft_config = config.raft_engine.config();
        let raft_engine =
            RaftLogEngine::new(raft_config, key_manager.clone(), get_io_rate_limiter())
                .expect("failed to open raft engine");

        if should_dump {
            let config_raftdb = &config.raftdb;
            let raft_db_opts = config_raftdb.build_opt(env.clone(), None);
            let raft_cf_opts = config_raftdb.build_cf_opts(block_cache);
            let raftdb = engine_rocks::util::new_engine_opt(
                &config.raft_store.raftdb_path,
                raft_db_opts,
                raft_cf_opts,
            )
            .expect("failed to open raftdb for migration");
            dump_raftdb_to_raft_engine(&raftdb, &raft_engine, 8 /* threads */);
            raftdb.stop();
            drop(raftdb);
            raft_data_state_machine.after_dump_data();
        }
        (raft_engine, None)
    }
}

impl ConfiguredRaftEngine for PSLogEngine {
    fn build(
        _config: &TikvConfig,
        _env: &Arc<Env>,
        _key_manager: &Option<Arc<DataKeyManager>>,
        _block_cache: &Cache,
    ) -> (Self, Option<Arc<RocksStatistics>>) {
        // create a dummy file in raft engine dir to pass initial config check
        let raft_engine_path = _config.raft_engine.config().dir + "/ps_engine";
        let path = Path::new(&raft_engine_path);
        if !path.exists() {
            File::create(path).unwrap();
        }
        (PSLogEngine::new(), None)
    }

    fn as_ps_engine(&mut self) -> Option<&mut PSLogEngine> {
        Some(self)
    }
}

/// Various sanity-checks and logging before running a server.
///
/// Warnings are logged.
///
/// # Logs
///
/// The presence of these environment variables that affect the database
/// behavior is logged.
///
/// - `GRPC_POLL_STRATEGY`
/// - `http_proxy` and `https_proxy`
///
/// # Warnings
///
/// - if `net.core.somaxconn` < 32768
/// - if `net.ipv4.tcp_syncookies` is not 0
/// - if `vm.swappiness` is not 0
/// - if data directories are not on SSDs
/// - if the "TZ" environment variable is not set on unix
fn pre_start() {
    check_environment_variables();
    for e in tikv_util::config::check_kernel() {
        warn!(
            "check: kernel";
            "err" => %e
        );
    }
}

fn check_system_config(config: &TikvConfig) {
    info!("beginning system configuration check");
    let mut rocksdb_max_open_files = config.rocksdb.max_open_files;
    if config.rocksdb.titan.enabled {
        // Titan engine maintains yet another pool of blob files and uses the same max
        // number of open files setup as rocksdb does. So we double the max required
        // open files here
        rocksdb_max_open_files *= 2;
    }
    if let Err(e) = tikv_util::config::check_max_open_fds(
        RESERVED_OPEN_FDS + (rocksdb_max_open_files + config.raftdb.max_open_files) as u64,
    ) {
        fatal!("{}", e);
    }

    // Check RocksDB data dir
    if let Err(e) = tikv_util::config::check_data_dir(&config.storage.data_dir) {
        warn!(
            "check: rocksdb-data-dir";
            "path" => &config.storage.data_dir,
            "err" => %e
        );
    }
    // Check raft data dir
    if let Err(e) = tikv_util::config::check_data_dir(&config.raft_store.raftdb_path) {
        warn!(
            "check: raftdb-path";
            "path" => &config.raft_store.raftdb_path,
            "err" => %e
        );
    }
}

fn try_lock_conflict_addr<P: AsRef<Path>>(path: P) -> File {
    let f = File::create(path.as_ref()).unwrap_or_else(|e| {
        fatal!(
            "failed to create lock at {}: {}",
            path.as_ref().display(),
            e
        )
    });

    if f.try_lock_exclusive().is_err() {
        fatal!(
            "{} already in use, maybe another instance is binding with this address.",
            path.as_ref().file_name().unwrap().to_str().unwrap()
        );
    }
    f
}

#[cfg(unix)]
fn get_lock_dir() -> String {
    format!("{}_TIKV_LOCK_FILES", unsafe { libc::getuid() })
}

#[cfg(not(unix))]
fn get_lock_dir() -> String {
    "TIKV_LOCK_FILES".to_owned()
}

/// A small trait for components which can be trivially stopped. Lets us keep
/// a list of these in `TiKV`, rather than storing each component individually.
trait Stop {
    fn stop(self: Box<Self>);
}

impl<E, R> Stop for StatusServer<E, R>
where
    E: 'static,
    R: 'static + Send,
{
    fn stop(self: Box<Self>) {
        (*self).stop()
    }
}

impl Stop for Worker {
    fn stop(self: Box<Self>) {
        Worker::stop(&self);
    }
}

impl<T: fmt::Display + Send + 'static> Stop for LazyWorker<T> {
    fn stop(self: Box<Self>) {
        self.stop_worker();
    }
}

pub struct EngineMetricsManager<EK: KvEngine, ER: RaftEngine> {
    tablet_registry: TabletRegistry<EK>,
    kv_statistics: Option<Arc<RocksStatistics>>,
    kv_is_titan: bool,
    raft_engine: ER,
    raft_statistics: Option<Arc<RocksStatistics>>,
    last_reset: Instant,
}

impl<EK: KvEngine, ER: RaftEngine> EngineMetricsManager<EK, ER> {
    pub fn new(
        tablet_registry: TabletRegistry<EK>,
        kv_statistics: Option<Arc<RocksStatistics>>,
        kv_is_titan: bool,
        raft_engine: ER,
        raft_statistics: Option<Arc<RocksStatistics>>,
    ) -> Self {
        EngineMetricsManager {
            tablet_registry,
            kv_statistics,
            kv_is_titan,
            raft_engine,
            raft_statistics,
            last_reset: Instant::now(),
        }
    }

    pub fn flush(&mut self, now: Instant) {
        let mut reporter = EK::StatisticsReporter::new("kv");
        self.tablet_registry
            .for_each_opened_tablet(|_, db: &mut CachedTablet<EK>| {
                if let Some(db) = db.latest() {
                    reporter.collect(db);
                }
                true
            });
        reporter.flush();
        self.raft_engine.flush_metrics("raft");

        if let Some(s) = self.kv_statistics.as_ref() {
            flush_engine_statistics(s, "kv", self.kv_is_titan);
        }
        if let Some(s) = self.raft_statistics.as_ref() {
            flush_engine_statistics(s, "raft", false);
        }
        if now.saturating_duration_since(self.last_reset) >= DEFAULT_ENGINE_METRICS_RESET_INTERVAL {
            if let Some(s) = self.kv_statistics.as_ref() {
                s.reset();
            }
            if let Some(s) = self.raft_statistics.as_ref() {
                s.reset();
            }
            self.last_reset = now;
        }
    }
}

pub struct EnginesResourceInfo {
    kv_engine: TiFlashEngine,
    raft_engine: Option<RocksEngine>,
    latest_normalized_pending_bytes: AtomicU32,
    normalized_pending_bytes_collector: MovingAvgU32,
}

impl EnginesResourceInfo {
    const SCALE_FACTOR: u64 = 100;

    fn new<CER: ConfiguredRaftEngine>(
        engines: &Engines<TiFlashEngine, CER>,
        max_samples_to_preserve: usize,
    ) -> Self {
        let raft_engine = engines.raft.as_rocks_engine().cloned();
        EnginesResourceInfo {
            kv_engine: engines.kv.clone(),
            raft_engine,
            latest_normalized_pending_bytes: AtomicU32::new(0),
            normalized_pending_bytes_collector: MovingAvgU32::new(max_samples_to_preserve),
        }
    }

    pub fn update(&self, _now: Instant) {
        let mut normalized_pending_bytes = 0;

        fn fetch_engine_cf(engine: &RocksEngine, cf: &str, normalized_pending_bytes: &mut u32) {
            if let Ok(cf_opts) = engine.get_options_cf(cf) {
                if let Ok(Some(b)) = engine.get_cf_pending_compaction_bytes(cf) {
                    if cf_opts.get_soft_pending_compaction_bytes_limit() > 0 {
                        *normalized_pending_bytes = std::cmp::max(
                            *normalized_pending_bytes,
                            (b * EnginesResourceInfo::SCALE_FACTOR
                                / cf_opts.get_soft_pending_compaction_bytes_limit())
                                as u32,
                        );
                    }
                }
            }
        }

        if let Some(raft_engine) = &self.raft_engine {
            fetch_engine_cf(raft_engine, CF_DEFAULT, &mut normalized_pending_bytes);
        }
        for cf in &[CF_DEFAULT, CF_WRITE, CF_LOCK] {
            fetch_engine_cf(&self.kv_engine.rocks, cf, &mut normalized_pending_bytes);
        }
        let (_, avg) = self
            .normalized_pending_bytes_collector
            .add(normalized_pending_bytes);
        self.latest_normalized_pending_bytes.store(
            std::cmp::max(normalized_pending_bytes, avg),
            Ordering::Relaxed,
        );
    }
}

impl IoBudgetAdjustor for EnginesResourceInfo {
    fn adjust(&self, total_budgets: usize) -> usize {
        let score = self.latest_normalized_pending_bytes.load(Ordering::Relaxed) as f32
            / Self::SCALE_FACTOR as f32;
        // Two reasons for adding `sqrt` on top:
        // 1) In theory the convergence point is independent of the value of pending
        //    bytes (as long as backlog generating rate equals consuming rate, which is
        //    determined by compaction budgets), a convex helps reach that point while
        //    maintaining low level of pending bytes.
        // 2) Variance of compaction pending bytes grows with its magnitude, a filter
        //    with decreasing derivative can help balance such trend.
        let score = score.sqrt();
        // The target global write flow slides between Bandwidth / 2 and Bandwidth.
        let score = 0.5 + score / 2.0;
        (total_budgets as f32 * score) as usize
    }
}
