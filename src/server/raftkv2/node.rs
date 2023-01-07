// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

use std::sync::{Arc, Mutex};

use causal_ts::CausalTsProviderImpl;
use concurrency_manager::ConcurrencyManager;
use engine_traits::{KvEngine, RaftEngine, TabletContext, TabletRegistry};
use kvproto::{metapb, replication_modepb::ReplicationStatus};
use pd_client::PdClient;
use raftstore::{
    coprocessor::CoprocessorHost,
    store::{GlobalReplicationState, TabletSnapManager, Transport, RAFT_INIT_LOG_INDEX},
};
use raftstore_v2::{router::RaftRouter, Bootstrap, PdTask, StoreSystem};
use slog::{info, o, Logger};
use tikv_util::{
    config::VersionTrack,
    worker::{LazyWorker, Worker},
};

use crate::server::{node::init_store, Result};

// TODO: we will rename another better name like RaftStore later.
pub struct NodeV2<C: PdClient + 'static, EK: KvEngine, ER: RaftEngine> {
    cluster_id: u64,
    store: metapb::Store,
    system: Option<(RaftRouter<EK, ER>, StoreSystem<EK, ER>)>,
    has_started: bool,

    pd_client: Arc<C>,
    registry: TabletRegistry<EK>,
    logger: Logger,
}

impl<C, EK, ER> NodeV2<C, EK, ER>
where
    C: PdClient,
    EK: KvEngine,
    ER: RaftEngine,
{
    /// Creates a new Node.
    pub fn new(
        cfg: &crate::server::Config,
        pd_client: Arc<C>,
        store: Option<metapb::Store>,
        registry: TabletRegistry<EK>,
    ) -> NodeV2<C, EK, ER> {
        let store = init_store(store, cfg);

        NodeV2 {
            cluster_id: cfg.cluster_id,
            store,
            pd_client,
            system: None,
            has_started: false,
            registry,
            logger: slog_global::borrow_global().new(o!()),
        }
    }

    pub fn try_bootstrap_store(
        &mut self,
        cfg: &raftstore_v2::Config,
        raft_engine: &ER,
    ) -> Result<()> {
        let store_id = Bootstrap::new(
            raft_engine,
            self.cluster_id,
            &*self.pd_client,
            self.logger.clone(),
        )
        .bootstrap_store()?;
        self.store.set_id(store_id);
        let (router, system) =
            raftstore_v2::create_store_batch_system(cfg, store_id, self.logger.clone());
        self.system = Some((
            RaftRouter::new(store_id, self.registry.clone(), router),
            system,
        ));
        Ok(())
    }

    pub fn router(&self) -> &RaftRouter<EK, ER> {
        &self.system.as_ref().unwrap().0
    }

    /// Starts the Node. It tries to bootstrap cluster if the cluster is not
    /// bootstrapped yet. Then it spawns a thread to run the raftstore in
    /// background.
    pub fn start<T>(
        &mut self,
        raft_engine: ER,
        trans: T,
        snap_mgr: TabletSnapManager,
        concurrency_manager: ConcurrencyManager,
        causal_ts_provider: Option<Arc<CausalTsProviderImpl>>, // used for rawkv apiv2
        coprocessor_host: CoprocessorHost<EK>,
        background: Worker,
        pd_worker: LazyWorker<PdTask>,
        store_cfg: Arc<VersionTrack<raftstore_v2::Config>>,
        state: &Mutex<GlobalReplicationState>,
    ) -> Result<()>
    where
        T: Transport + 'static,
    {
        let store_id = self.id();
        if let Some(region) = Bootstrap::new(
            &raft_engine,
            self.cluster_id,
            &*self.pd_client,
            self.logger.clone(),
        )
        .bootstrap_first_region(&self.store, store_id)?
        {
            let path = self
                .registry
                .tablet_path(region.get_id(), RAFT_INIT_LOG_INDEX);
            let ctx = TabletContext::new(&region, Some(RAFT_INIT_LOG_INDEX));
            // TODO: make follow line can recover from abort.
            self.registry
                .tablet_factory()
                .open_tablet(ctx, &path)
                .unwrap();
        }

        // Put store only if the cluster is bootstrapped.
        info!(self.logger, "put store to PD"; "store" => ?&self.store);
        let status = self.pd_client.put_store(self.store.clone())?;
        self.load_all_stores(state, status);

        self.start_store(
            raft_engine,
            trans,
            snap_mgr,
            concurrency_manager,
            causal_ts_provider,
            coprocessor_host,
            background,
            pd_worker,
            store_cfg,
        )?;

        Ok(())
    }

    /// Gets the store id.
    pub fn id(&self) -> u64 {
        self.store.get_id()
    }

    pub fn logger(&self) -> Logger {
        self.logger.clone()
    }

    /// Gets a copy of Store which is registered to Pd.
    pub fn store(&self) -> metapb::Store {
        self.store.clone()
    }

    // TODO: support updating dynamic configuration.

    // TODO: check api version.
    // Do we really need to do the check giving we don't consider support upgrade
    // ATM?

    fn load_all_stores(
        &mut self,
        state: &Mutex<GlobalReplicationState>,
        status: Option<ReplicationStatus>,
    ) {
        info!(self.logger, "initializing replication mode"; "status" => ?status, "store_id" => self.store.id);
        let stores = match self.pd_client.get_all_stores(false) {
            Ok(stores) => stores,
            Err(e) => panic!("failed to load all stores: {:?}", e),
        };
        let mut state = state.lock().unwrap();
        if let Some(s) = status {
            state.set_status(s);
        }
        for mut store in stores {
            state
                .group
                .register_store(store.id, store.take_labels().into());
        }
    }

    fn start_store<T>(
        &mut self,
        raft_engine: ER,
        trans: T,
        snap_mgr: TabletSnapManager,
        concurrency_manager: ConcurrencyManager,
        causal_ts_provider: Option<Arc<CausalTsProviderImpl>>, // used for rawkv apiv2
        coprocessor_host: CoprocessorHost<EK>,
        background: Worker,
        pd_worker: LazyWorker<PdTask>,
        store_cfg: Arc<VersionTrack<raftstore_v2::Config>>,
    ) -> Result<()>
    where
        T: Transport + 'static,
    {
        let store_id = self.store.get_id();
        info!(self.logger, "start raft store thread"; "store_id" => store_id);

        if self.has_started {
            return Err(box_err!("{} is already started", store_id));
        }
        self.has_started = true;

        let (router, system) = self.system.as_mut().unwrap();

        system.start(
            store_id,
            store_cfg,
            raft_engine,
            self.registry.clone(),
            trans,
            self.pd_client.clone(),
            router.store_router(),
            router.store_meta().clone(),
            snap_mgr,
            concurrency_manager,
            causal_ts_provider,
            coprocessor_host,
            background,
            pd_worker,
        )?;
        Ok(())
    }

    /// Stops the Node.
    pub fn stop(&mut self) {
        let store_id = self.store.get_id();
        let Some((_, mut system)) = self.system.take() else { return };
        info!(self.logger, "stop raft store thread"; "store_id" => store_id);
        system.shutdown();
    }
}
