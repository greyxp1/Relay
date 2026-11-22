use pipewire::{
    self as pw, context::ContextRc, node::Node, registry::RegistryRc, sys,
    thread_loop::ThreadLoopRc,
};
use std::{
    cell::{Cell, RefCell, UnsafeCell},
    ffi::{c_void, CString},
    io,
    mem::MaybeUninit,
    ptr::{self, NonNull},
    rc::Rc,
};

/// Routes application audio into a private sink for screenshare capture.
pub(crate) struct ShareAudio {
    _registry_listener: pw::registry::Listener,
    _core_listener: pw::core::Listener,
    graph: Rc<Graph>,
    _sink: Node,
    _registry: RegistryRc,
}

// Once started, callbacks run under the PipeWire loop lock. The owner takes
// that lock to read errors and joins the loop before releasing any graph objects.
unsafe impl Send for ShareAudio {}

impl ShareAudio {
    pub(crate) fn open(name: String) -> Result<Self, Box<dyn std::error::Error>> {
        // SAFETY: the loop is stopped before any objects that use it are dropped.
        let loop_ = unsafe { ThreadLoopRc::new_cstr(Some(c"relay-share-audio"), None)? };
        let context = ContextRc::new(&loop_, None)?;
        let core = context.connect_rc(None).map_err(creation_error)?;
        let registry = core.get_registry_rc()?;
        let graph = Rc::new(Graph {
            loop_,
            context,
            targets: RefCell::default(),
            source: Cell::new(None),
            error: Cell::new(0),
            name,
        });
        let globals = Rc::clone(&graph);
        let removals = Rc::clone(&graph);
        let nodes = registry.clone();
        let registry_listener = registry
            .add_listener_local()
            .global(move |object| globals.global(&nodes, object))
            .global_remove(move |id| {
                removals
                    .targets
                    .borrow_mut()
                    .retain(|target| target.id != id);
                if removals.source.get() == Some(id) {
                    removals.fail(-libc::EPIPE);
                }
            })
            .register();
        let errors = Rc::clone(&graph);
        let core_listener = core
            .add_listener_local()
            .error(move |id, _, result, _| {
                if id == pw::core::PW_ID_CORE && result < 0 {
                    errors.fail(result);
                }
            })
            .register();
        let sink = core
            .create_object::<Node>(
                "adapter",
                &pw::properties::properties! {
                    "factory.name" => "support.null-audio-sink",
                    "node.name" => graph.name.as_str(),
                    "node.description" => "Relay share audio",
                    "media.class" => "Audio/Sink",
                    "node.virtual" => "true",
                    "priority.session" => "-1000",
                    "audio.position" => "[ FL FR ]",
                },
            )
            .map_err(creation_error)?;
        let audio = Self {
            _registry_listener: registry_listener,
            _core_listener: core_listener,
            graph,
            _sink: sink,
            _registry: registry,
        };
        drop(core);
        audio.wait_for_sink()?;
        Ok(audio)
    }

    fn wait_for_sink(&self) -> io::Result<()> {
        let loop_ = &self.graph.loop_;
        let _lock = loop_.lock();
        let raw = loop_.as_raw_ptr();
        let mut deadline = MaybeUninit::uninit();
        // The safe wrappers discard return codes needed to distinguish failure from readiness.
        // SAFETY: the loop is owned and locked, and get_time initializes the deadline on success.
        unsafe {
            check(sys::pw_thread_loop_start(raw))?;
            check(sys::pw_thread_loop_get_time(
                raw,
                deadline.as_mut_ptr(),
                5_000_000_000,
            ))?;
            let deadline = deadline.assume_init();
            let mut result = 0;
            while result >= 0 && self.graph.error.get() == 0 && self.graph.source.get().is_none() {
                result = sys::pw_thread_loop_timed_wait_full(raw, &deadline);
            }
            check(self.graph.error.get())?;
            check(result)
        }
    }

    pub(crate) fn error(&self) -> Option<io::Error> {
        let _lock = self.graph.loop_.lock();
        check(self.graph.error.get()).err()
    }
}

impl Drop for ShareAudio {
    fn drop(&mut self) {
        self.graph.loop_.stop();
    }
}

fn check(result: i32) -> io::Result<()> {
    if result < 0 {
        Err(io::Error::from_raw_os_error(result.saturating_neg()))
    } else {
        Ok(())
    }
}

fn creation_error(error: pw::Error) -> io::Error {
    if matches!(error, pw::Error::CreationFailed) {
        let os_error = io::Error::last_os_error();
        if os_error.raw_os_error() != Some(0) {
            return os_error;
        }
    }
    io::Error::other(error)
}

struct Graph {
    targets: RefCell<Vec<Target>>,
    loop_: ThreadLoopRc,
    context: ContextRc,
    source: Cell<Option<u32>>,
    error: Cell<i32>,
    name: String,
}

impl Graph {
    fn fail(&self, error: i32) {
        self.error.set(error);
        self.loop_.signal(false);
    }

    fn global(
        self: &Rc<Self>,
        registry: &RegistryRc,
        object: &pw::registry::GlobalObject<&pw::spa::utils::dict::DictRef>,
    ) {
        let Some(props) = object
            .props
            .filter(|_| object.type_ == pw::types::ObjectType::Node)
        else {
            return;
        };
        let name = props.get("node.name").unwrap_or_default();
        if name == self.name {
            self.source.set(Some(object.id));
            for target in self.targets.borrow().iter() {
                target.route.connect(self);
            }
            self.loop_.signal(false);
        } else if props.get("media.class") == Some("Stream/Output/Audio")
            && !matches!(name, "relay-playback" | "relay-stream-playback")
            && !name.starts_with("relay-share-")
        {
            let Some(serial) = props
                .get("object.serial")
                .and_then(|serial| serial.parse().ok())
            else {
                return;
            };
            let Ok(node) = registry.bind::<Node, _>(object) else {
                return;
            };
            let route = Rc::new(Route {
                serial,
                eligible: Cell::new(false),
                module: Cell::new(None),
                listener: UnsafeCell::new(MaybeUninit::uninit()),
            });
            let graph = Rc::downgrade(self);
            let observed = Rc::clone(&route);
            let listener = node
                .add_listener_local()
                .info(move |info| {
                    if !info.change_mask().contains(pw::node::NodeChangeMask::PROPS) {
                        return;
                    }
                    if let (Some(props), Some(graph)) = (info.props(), graph.upgrade()) {
                        // Registry globals omit node.virtual. Effects outputs can include call playback.
                        observed
                            .eligible
                            .set(!matches!(props.get("node.virtual"), Some("true" | "1")));
                        if observed.eligible.get() {
                            observed.connect(&graph);
                        } else {
                            observed.disconnect();
                        }
                    }
                })
                .register();
            self.targets.borrow_mut().push(Target {
                id: object.id,
                route,
                _listener: listener,
                _node: node,
            });
        }
    }
}

struct Target {
    id: u32,
    route: Rc<Route>,
    _listener: pw::node::NodeListener,
    _node: Node,
}

impl Drop for Target {
    fn drop(&mut self) {
        self.route.disconnect();
    }
}

struct Route {
    serial: u64,
    eligible: Cell<bool>,
    module: Cell<Option<NonNull<sys::pw_impl_module>>>,
    listener: UnsafeCell<MaybeUninit<pw::spa::sys::spa_hook>>,
}

impl Route {
    fn connect(&self, graph: &Graph) {
        if graph.source.get().is_none() || !self.eligible.get() || self.module.get().is_some() {
            return;
        }
        let name = &graph.name;
        let serial = self.serial;
        // Object serials distinguish streams with identical names, including browser tabs.
        let args = CString::new(format!(
            "{{ audio.position=[ FL FR ] \
             capture.props={{ target.object=\"{serial}\" node.name={name}.capture.{serial} \
             node.passive=true node.dont-fallback=true node.linger=true }} \
             playback.props={{ target.object={name} node.name={name}.forward.{serial} \
             node.passive=true node.dont-fallback=true node.linger=true }} }}"
        ))
        .expect("Relay-generated sink name");
        static EVENTS: sys::pw_impl_module_events = sys::pw_impl_module_events {
            version: sys::PW_VERSION_IMPL_MODULE_EVENTS,
            destroy: Some(module_destroyed),
            free: None,
            initialized: None,
            registered: None,
        };
        // SAFETY: the module uses the live graph context. The Rc allocation keeps the
        // route and its hook at fixed addresses through module destruction.
        unsafe {
            let module = sys::pw_context_load_module(
                graph.context.as_raw_ptr(),
                c"libpipewire-module-loopback".as_ptr(),
                args.as_ptr(),
                ptr::null_mut(),
            );
            if let Some(module) = NonNull::new(module) {
                self.module.set(Some(module));
                sys::pw_impl_module_add_listener(
                    module.as_ptr(),
                    self.listener.get().cast::<pw::spa::sys::spa_hook>(),
                    &EVENTS,
                    ptr::from_ref(self).cast_mut().cast(),
                );
            }
        }
    }

    fn disconnect(&self) {
        if let Some(module) = self.module.take() {
            // SAFETY: the loop serializes teardown, and the Route outlives the callback.
            unsafe { sys::pw_impl_module_destroy(module.as_ptr()) };
        }
    }
}

unsafe extern "C" fn module_destroyed(data: *mut c_void) {
    // SAFETY: PipeWire calls this while the route is alive and unlinks the hook before returning.
    unsafe { &*data.cast::<Route>() }.module.set(None);
}
