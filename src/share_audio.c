#include <pipewire/impl.h>
#include <stdio.h>

struct target {
    struct spa_list link;
    uint32_t id;
    uint64_t serial;
    struct pw_impl_module *module;
    struct spa_hook listener;
    struct pw_node *node;
    struct spa_hook node_listener;
    struct share_audio *owner;
    bool eligible;
};

struct share_audio {
    struct pw_thread_loop *loop;
    struct pw_context *context;
    struct pw_core *core;
    struct pw_registry *registry;
    struct pw_node *sink;
    struct spa_hook core_listener, registry_listener;
    struct spa_list targets;
    uint32_t source;
    int error;
    char *name;
};

static void module_destroyed(void *data)
{
    struct target *t = data;
    spa_hook_remove(&t->listener);
    t->module = NULL;
}

static void destroy_target(struct target *t)
{
    if (t->module)
        pw_impl_module_destroy(t->module);
    spa_hook_remove(&t->node_listener);
    pw_proxy_destroy((struct pw_proxy *)t->node);
    spa_list_remove(&t->link);
    free(t);
}

static void connect_target(struct share_audio *s, struct target *t)
{
    static const struct pw_impl_module_events events = {
        PW_VERSION_IMPL_MODULE_EVENTS, .destroy = module_destroyed
    };
    if (s->source == PW_ID_ANY || !t->eligible || t->module)
        return;
    char args[1024];
    // Browser tabs can have identical node names. Copy their unique streams without moving them.
    snprintf(args, sizeof(args),
             "{ audio.position=[ FL FR ] "
             "capture.props={ target.object=\"%" PRIu64 "\" node.name=%s.capture.%" PRIu64
             " node.passive=true node.dont-fallback=true node.linger=true } "
             "playback.props={ target.object=%s node.name=%s.forward.%" PRIu64
             " node.passive=true node.dont-fallback=true node.linger=true } }",
             t->serial, s->name, t->serial, s->name, s->name, t->serial);
    t->module = pw_context_load_module(s->context, "libpipewire-module-loopback", args, NULL);
    if (!t->module)
        s->error = errno;
    else
        pw_impl_module_add_listener(t->module, &t->listener, &events, t);
}

static void node_info(void *data, const struct pw_node_info *info)
{
    struct target *t = data;
    if (!(info->change_mask & PW_NODE_CHANGE_MASK_PROPS) || !info->props)
        return;
    // Registry globals omit node.virtual. Effects outputs already mix in call playback.
    t->eligible = !spa_atob(spa_dict_lookup(info->props, PW_KEY_NODE_VIRTUAL));
    if (!t->eligible && t->module)
        pw_impl_module_destroy(t->module);
    connect_target(t->owner, t);
}

static void global(void *data, uint32_t id, uint32_t permissions,
                   const char *type, uint32_t version, const struct spa_dict *props)
{
    struct share_audio *s = data;
    (void)permissions;
    static const struct pw_node_events node_events = {
        PW_VERSION_NODE_EVENTS, .info = node_info
    };
    if (!props || !spa_streq(type, PW_TYPE_INTERFACE_Node))
        return;
    const char *name = spa_dict_lookup(props, PW_KEY_NODE_NAME);
    const char *media = spa_dict_lookup(props, PW_KEY_MEDIA_CLASS);
    const char *serial = spa_dict_lookup(props, PW_KEY_OBJECT_SERIAL);
    if (spa_streq(name, s->name)) {
        s->source = id;
        struct target *t;
        spa_list_for_each(t, &s->targets, link)
            connect_target(s, t);
    } else if (spa_streq(media, "Stream/Output/Audio") && serial &&
               !spa_streq(name, "relay-playback") &&
               !(name && spa_strstartswith(name, "relay-share-"))) {
        struct target *t = calloc(1, sizeof(*t));
        if (!t) {
            s->error = ENOMEM;
        } else {
            t->id = id;
            t->serial = strtoull(serial, NULL, 10);
            t->owner = s;
            t->node = pw_registry_bind(s->registry, id, type, SPA_MIN(version, (uint32_t)PW_VERSION_NODE), 0);
            if (!t->node) {
                s->error = errno;
                free(t);
            } else {
                spa_list_append(&s->targets, &t->link);
                pw_node_add_listener(t->node, &t->node_listener, &node_events, t);
            }
        }
    }
    pw_thread_loop_signal(s->loop, false);
}

static void global_remove(void *data, uint32_t id)
{
    struct share_audio *s = data;
    struct target *t, *next;
    spa_list_for_each_safe(t, next, &s->targets, link) {
        if (t->id == id) {
            destroy_target(t);
        }
    }
    if (id == s->source)
        s->error = EPIPE;
}

static void core_error(void *data, uint32_t id, int seq, int res, const char *message)
{
    struct share_audio *s = data;
    (void)seq;
    (void)message;
    if (id == PW_ID_CORE && res < 0) {
        s->error = -res;
        pw_thread_loop_signal(s->loop, false);
    }
}

void relay_share_audio_free(struct share_audio *s)
{
    if (!s)
        return;
    if (s->loop)
        pw_thread_loop_stop(s->loop);
    struct target *t, *next;
    spa_list_for_each_safe(t, next, &s->targets, link) {
        destroy_target(t);
    }
    if (s->sink)
        pw_proxy_destroy((struct pw_proxy *)s->sink);
    if (s->registry)
        pw_proxy_destroy((struct pw_proxy *)s->registry);
    if (s->core)
        pw_core_disconnect(s->core);
    if (s->context)
        pw_context_destroy(s->context);
    if (s->loop)
        pw_thread_loop_destroy(s->loop);
    free(s->name);
    free(s);
}

int relay_share_audio_open(const char *name, struct share_audio **result)
{
    static const struct pw_registry_events registry_events = {
        PW_VERSION_REGISTRY_EVENTS, .global = global, .global_remove = global_remove
    };
    static const struct pw_core_events core_events = {
        PW_VERSION_CORE_EVENTS, .error = core_error
    };
    pw_init(NULL, NULL);
    struct share_audio *s = calloc(1, sizeof(*s));
    if (!s)
        return -ENOMEM;
    spa_list_init(&s->targets);
    s->source = PW_ID_ANY;
    s->name = strdup(name);
    int error = ENOMEM;
    if (!s->name || !(s->loop = pw_thread_loop_new("relay-share-audio", NULL)) ||
        !(s->context = pw_context_new(pw_thread_loop_get_loop(s->loop), NULL, 0)))
        goto fail;
    if (!(s->core = pw_context_connect(s->context, NULL, 0))) {
        error = errno;
        goto fail;
    }
    pw_core_add_listener(s->core, &s->core_listener, &core_events, s);
    s->registry = pw_core_get_registry(s->core, PW_VERSION_REGISTRY, 0);
    if (!s->registry)
        goto fail;
    pw_registry_add_listener(s->registry, &s->registry_listener, &registry_events, s);
    struct pw_properties *props = pw_properties_new(
        PW_KEY_FACTORY_NAME, "support.null-audio-sink",
        PW_KEY_NODE_NAME, name, PW_KEY_NODE_DESCRIPTION, "Relay share audio",
        PW_KEY_MEDIA_CLASS, "Audio/Sink", PW_KEY_NODE_VIRTUAL, "true",
        PW_KEY_PRIORITY_SESSION, "-1000", "audio.position", "[ FL FR ]", NULL);
    if (!props)
        goto fail;
    s->sink = pw_core_create_object(s->core, "adapter", PW_TYPE_INTERFACE_Node,
                                    PW_VERSION_NODE, &props->dict, 0);
    pw_properties_free(props);
    if (!s->sink) {
        error = errno;
        goto fail;
    }
    pw_thread_loop_lock(s->loop);
    int res = pw_thread_loop_start(s->loop);
    struct timespec deadline;
    pw_thread_loop_get_time(s->loop, &deadline, 5 * SPA_NSEC_PER_SEC);
    while (res >= 0 && !s->error && s->source == PW_ID_ANY)
        res = pw_thread_loop_timed_wait_full(s->loop, &deadline);
    error = s->error ? s->error : res < 0 ? -res : 0;
    pw_thread_loop_unlock(s->loop);
    if (error)
        goto fail;
    *result = s;
    return 0;
fail:
    relay_share_audio_free(s);
    return -error;
}

int relay_share_audio_error(struct share_audio *s)
{
    pw_thread_loop_lock(s->loop);
    int error = s->error;
    pw_thread_loop_unlock(s->loop);
    return error;
}
