/*
 * Sharing shortcuts and preview cleanup adapted from Equicord.
 * Copyright (c) 2024–2025 Vendicated and contributors
 * SPDX-License-Identifier: GPL-3.0-or-later
 */
import { definePluginSettings } from "@api/Settings";
import definePlugin, { OptionType } from "@utils/types";
import { find, findByCodeLazy } from "@webpack";
import { ChannelActions, ChannelStore, FluxDispatcher, MediaEngineStore, React, SelectedChannelStore, showToast, UserStore, VoiceStateStore } from "@webpack/common";
import { BrowserPlayback } from "./playback";
import { closeShareDialog, getShareQuality, RelaySettings, saveShareQuality, selectShare } from "./share";
import workerSource from "file://audio-bridge.js";
import videoWorkerSource from "file://video-worker.js";

type RTC = {
    userId: string;
    sessionId: string;
    guildId?: string;
    channelId: string;
    trueChannelId: string;
    trueServerId: string;
    context: string;
    endpoint: string | null;
    token: string | null;
    _channelId: string;
    _nextChannelId?: string;
    _destroyed: boolean;
    _connectCompletedTime: number;
    _mediaEngineConnectionId: string | null;
    _hasEverConnected: boolean;
    _connecting: boolean;
    _cleanupSocket(): void;
    _cancelReconnect(): void;
    setState(state: string): void;
    isStreamSender(): boolean;
    getMediaStreamKey(): string;
    getExtraConnectionOptions(): { streamUserId?: string };
    emit(event: string, ...args: unknown[]): void;
    _handlePing(ping: number): void;
};

type Connection = { id: string; rtc: RTC; owner?: string; source?: string; ready: boolean; started: boolean };
type Screen = { stream: MediaStream; id: string };

const settings = definePluginSettings({
    source: { type: OptionType.STRING, description: "Video device to share, empty for screen or window picker", hidden: true, default: "" },
    autoShare: { type: OptionType.BOOLEAN, description: "Start sharing with saved settings when joining voice", hidden: true, default: false },
    preferences: { type: OptionType.COMPONENT, component: (): React.ReactNode => React.createElement(RelaySettings, { settings }) },
});
const connections = new Map<RTC, Connection>();
const stopStream = findByCodeLazy('type:"STREAM_STOP"');
const startStream = findByCodeLazy('type:"STREAM_START"');
const screens = new Map<string, Screen>();
let playback: BrowserPlayback | undefined;
let starting: Promise<BrowserPlayback> | undefined;
let active = false;
let speakingTimer: ReturnType<typeof setInterval> | undefined;
let lastSpeakers = new Set<string>();
let captureTrack: MediaStreamTrack | undefined;
let preparing: { id: string; resolve: () => void; reject: (error: Error) => void } | undefined;
let originalCapture: typeof navigator.mediaDevices.getDisplayMedia;
let registry: { add: (stream: MediaStream) => string; remove: (id: string) => void };
let urls: string[] = [];
let automaticCapture = false;
let sharing = false;

function toast(text: string) { showToast({ text }); }
function send(message: object) { playback?.socket?.postMessage(JSON.stringify(message)); }
function blob(source: string) {
    const url = URL.createObjectURL(new Blob([source], { type: "text/javascript" }));
    urls.push(url);
    return url;
}
function fail(message: string) {
    if (!active) return;
    preparing?.reject(new Error(message));
    preparing = undefined;
    for (const connection of connections.values()) {
        connection.rtc.setState("RTC_DISCONNECTED");
        connection.ready = false;
    }
    showToast({ text: "Relay: " + message, variant: "critical" });
    closePlayback();
}
function closePlayback() {
    closeShareDialog();
    preparing?.reject(new DOMException("Voice disconnected", "AbortError"));
    preparing = undefined;
    clearInterval(speakingTimer);
    speakingTimer = undefined;
    const current = playback;
    playback = undefined;
    starting = undefined;
    endCapture();
    current?.close();
    for (const screen of screens.values()) {
        registry?.remove(screen.id);
        screen.stream.getTracks().forEach(track => track.stop());
    }
    screens.clear();
    for (const url of urls) URL.revokeObjectURL(url);
    urls = [];
    for (const user of lastSpeakers) FluxDispatcher.dispatch({ type: "SPEAKING", context: "default", userId: user, speakingFlags: 0 });
    lastSpeakers.clear();
}
function control(message: any) {
    if (message.op === "cameraEnded") {
        const owner = "camera:" + message.user;
        const screen = screens.get(owner);
        if (screen) {
            registry.remove(screen.id);
            screen.stream.getTracks().forEach(track => track.stop());
            screens.delete(owner);
            playback?.removeScreen(owner);
        }
        return;
    }
    if (message.op === "deviceError") { toast("Relay: " + message.error); return; }
    if (message.op === "captureEnded") {
        endCapture();
        if (message.error) toast("Relay: " + message.error);
        return;
    }
    if (message.op === "prepared") {
        if (!preparing || preparing.id !== message.id) return;
        const pending = preparing;
        preparing = undefined;
        if (message.error) pending.reject(new Error(message.error));
        else pending.resolve();
        return;
    }
    const connection = [...connections.values()].find(c => c.id === message.id);
    if (!connection) return;
    if (message.op === "speakers") {
        const speakers = new Set<string>(message.users);
        for (const user of new Set([...speakers, ...lastSpeakers])) {
            if (speakers.has(user) === lastSpeakers.has(user)) continue;
            const flags = speakers.has(user) ? 1 : 0;
            connection.rtc.emit("speaking", user, flags);
            FluxDispatcher.dispatch({ type: "SPEAKING", context: "default", userId: user, speakingFlags: flags });
        }
        lastSpeakers = speakers;
    } else if (message.op === "ended") {
        if (connection.rtc.isStreamSender()) endCapture();
        connection.ready = false;
        connection.rtc.setState("RTC_DISCONNECTED");
        if (message.error) toast("Relay: " + message.error);
    } else if (message.op === "state") {
        const ready = !!message.ready;
        if (ready !== connection.ready) {
            connection.ready = ready;
            if (ready) {
                connection.rtc._connectCompletedTime = performance.now();
                connection.rtc._hasEverConnected = true;
                connection.rtc._connecting = false;
                const screen = connection.owner && screens.get(connection.owner);
                if (screen) connection.rtc.emit("video", connection.rtc.guildId, connection.rtc.channelId, connection.owner, screen.id, connection.rtc.trueServerId);
            }
            connection.rtc.setState(ready ? "RTC_CONNECTED" : "RTC_CONNECTING");
            if (ready && !connection.started) {
                connection.started = true;
                if (connection.rtc.context === "default" && settings.store.autoShare) queueMicrotask(() => plugin.share(true));
            }
        }
        if (ready) connection.rtc._handlePing(message.ping);
    }
}
function screenStream(owner: string, stream: MediaStream) {
    let screen = screens.get(owner);
    if (!screen) {
        stream = stream.clone();
        screen = { stream, id: registry.add(stream) };
        screens.set(owner, screen);
        for (const connection of connections.values()) {
            if (owner.startsWith("camera:") ? !connection.owner && connection.rtc.context === "default" : connection.owner === owner) {
                connection.rtc.emit("video", connection.rtc.guildId, connection.rtc.channelId, owner.replace(/^camera:/, ""), screen.id, connection.rtc.trueServerId);
            }
        }
    }
    return screen;
}
async function ensurePlayback() {
    if (starting) return starting;
    starting = (async () => {
        const current = new BrowserPlayback((message, owner, recover) => {
            if (playback !== current) return;
            if (recover && owner) {
                const connection = [...connections.values()].find(c => owner.startsWith("camera:") ? !c.owner && c.rtc.context === "default" : c.owner === owner);
                if (connection) send({ op: "keyframe", id: connection.id });
            } else if (message) {
                if (owner) toast("Relay: " + message);
                else fail(message);
            }
        }, {
            url: "ws://127.0.0.1:38446/media",
            worker: blob(workerSource),
            control: message => { if (playback === current) control(message); },
            frame: (owner, stream) => { if (playback === current) screenStream(owner, stream); },
        });
        playback = current;
        try {
            await current.start();
        } catch {
            if (playback === current) closePlayback();
            else current.close();
            throw new Error("Could not connect to the companion. Check that Relay is running.");
        }
        if (playback !== current) return current;
        speakingTimer = setInterval(() => {
            if (captureTrack?.readyState === "ended") {
                send({ op: "cancelShare" });
                captureTrack = undefined;
            }
        }, 50);
        return current;
    })();
    return starting;
}
function discordAudioDevice(input: boolean): string {
    const id = input ? MediaEngineStore.getInputDeviceId() : MediaEngineStore.getOutputDeviceId();
    if (!id || id === "default") return "";
    const devices = input ? MediaEngineStore.getInputDevices() : MediaEngineStore.getOutputDevices();
    // Browser device IDs are origin-specific. The companion resolves unique OS device names.
    return devices[id]?.name || id;
}
function voiceState() {
    return { source: discordAudioDevice(true) || null, volume: MediaEngineStore.getInputVolume() / 100,
        mute: MediaEngineStore.isMute(), deaf: MediaEngineStore.isDeaf(),
        output: discordAudioDevice(false), outputVolume: MediaEngineStore.getOutputVolume() / 100,
        users: Object.fromEntries(Object.values(VoiceStateStore.getVoiceStatesForChannel(SelectedChannelStore.getVoiceChannelId()) ?? {})
            .map(state => [state.userId, voiceVolume(state.userId)]).filter(([, volume]) => volume !== 1)) };
}
function voiceVolume(user: string) {
    return MediaEngineStore.isLocalMute(user) ? 0 : MediaEngineStore.getLocalVolume(user) / 100;
}
function syncVoice() {
    if (!playback) return;
    const state = voiceState();
    const source = JSON.stringify(state);
    for (const connection of connections.values()) {
        if (!connection.owner) {
            if (source !== connection.source) { send({ op: "voice", id: connection.id, ...state }); connection.source = source; }
        }
    }
}

async function nativeCapture(options?: DisplayMediaStreamOptions): Promise<MediaStream> {
    if (!plugin.captures()) {
        return originalCapture.call(navigator.mediaDevices, options);
    }
    if (preparing) throw new DOMException("A screen picker is already open", "InvalidStateError");
    const changing = [...connections.values()].find(connection => connection.rtc.isStreamSender());
    const initial = { ...getShareQuality(), source: settings.store.source, autoShare: settings.store.autoShare };
    const interactive = !automaticCapture;
    const { source, autoShare, ...quality } = interactive ? await selectShare(initial, !!changing) : initial;
    const current = await ensurePlayback();
    const id = crypto.randomUUID();
    await new Promise<void>((resolve, reject) => {
        preparing = { id, resolve, reject };
        send({ op: "prepareShare", id, ...quality, source: source || null, replace: changing?.id });
    });
    // Discord uses this track for source lifetime and preview. Native media owns encoding.
    let stream: MediaStream;
    try {
        const owner = UserStore.getCurrentUser().id;
        stream = screenStream(owner, current.screen(owner).stream).stream.clone();
    } catch (error) {
        send({ op: "cancelShare" });
        throw error;
    }
    const track = stream.getVideoTracks()[0];
    if (!track) { send({ op: "cancelShare" }); throw new Error("Native preview is unavailable"); }
    if (interactive) {
        saveShareQuality(quality);
        settings.store.source = source;
        settings.store.autoShare = autoShare;
    }
    captureTrack?.stop();
    captureTrack = track;
    track.addEventListener("ended", () => { if (captureTrack === track) send({ op: "cancelShare" }); }, { once: true });
    return stream;
}

function endCapture() {
    const track = captureTrack;
    captureTrack = undefined;
    if (track) { send({ op: "cancelShare" }); track.stop(); track.dispatchEvent(new Event("ended")); }
    for (const { rtc } of [...connections.values()]) {
        if (rtc.isStreamSender()) stopStream(rtc.getMediaStreamKey());
    }
}

const plugin = definePlugin({
    name: "Relay",
    description: "Uses the local Relay companion for voice and screen sharing.",
    authors: [{ name: "greyxp1", id: 1233920168196046892n }],
    settings,
    dependencies: ["EquicordToolbox"],
    patches: [{
        // The desktop native engine cannot consume Relay's MediaStreams.
        find: '"cameraBackgroundLive"',
        replacement: {
            match: /\[(\i\.\i)\.NATIVE,\1\.WEBRTC\](?=\.find)/,
            replace: "[$1.WEBRTC]",
        },
    }, {
        find: "RTCConnection.connect(...): Attempting to connect on destroyed instance.",
        group: true,
        replacement: [
            { match: /connect\((\i),(\i)\)\{/, replace: "connect($1,$2){if($self.handles(this))return $self.connect(this,$1,$2);" },
            { match: /destroy\(\)\{(?=if\(this\.logger\.info\("Destroy RTCConnection"\))/, replace: "destroy(){$self.disconnect(this);" },
            { match: /reconnect=\(\)=>\{/, replace: "reconnect=()=>{if($self.owns(this))return $self.connect(this,this.endpoint?.replace(/^wss?:\\/\\//,'').replace(/\\/$/,''),this.token);" },
        ],
    }, {
        // Discord answers H.264 with constrained baseline, even when Chromium offers baseline first.
        find: ";level-asymmetry-allowed=1;packetization-mode=1;profile-level-id=42e01f",
        replacement: {
            match: /(function \i\((\i),(\i),\i,(\i),\i\)\{let \i=)\2\.find\((\i)=>\5\.codec===\4\)/,
            replace: "$1$self.videoCodec($2,$3,$4)",
        },
    }, {
        find: ";level-asymmetry-allowed=1;packetization-mode=1;profile-level-id=42e01f",
        group: true,
        replacement: [{
            // Advertise AV1 when Chromium includes it in its SDP offer.
            match: /(\[(?:\i\.\i\.H265,)?)(\i\.\i)\.H264,/g,
            replace: '$1"AV1",$2.H264,',
        }, {
            match: /(\i)\.setRemoteDescription\((\i)\)(?=\}catch\(\i\)\{this\.logger\.warn\(`Failed to set remote answer)/,
            replace: "$1.setRemoteDescription($self.videoAnswer($2,this.pc.localDescription))",
        }, {
            match: /new Worker\((new URL\("\/assets\/"\+\i\.u\(\d+\),\i\.b\)),(Object\.assign\(\{\},\{name:"encryption-worker"\},\{type:void 0\}\))\)/,
            replace: "$self.encryptionWorker($1,$2)",
        }],
    }, {
        // Browser audio starts disabled after reload. Native voice does not need that activation.
        find: 'static displayName="MediaEngineStore"',
        replacement: {
            match: /isEnabled\(\)\{return /,
            replace: "isEnabled(){return $self.usesNativeVoice()||",
        },
    }, {
        // Native voice can unmute before Discord finishes loading the icon animation.
        find: '"lottie_hover_multiple_loop"',
        group: true,
        replacement: [{
            match: /play:(\i)=>\{if\(null==(\i)\.current\)return;let (\i)=null==(\i)\.current;if\(\4\.current=\1,/,
            replace: "play:$1=>{let $3=null==$4.current;$4.current=$1;if(null==$2.current)return;if(",
        }, {
            match: /((\i)\.current\?\?.+?initialSegment:)(\i)/,
            replace: "$1null!=$2.current&&$3?[$3[1]-1,$3[1]]:$3",
        }],
    }, {
        find: 'static displayName="ApplicationStreamingSettingsStore"',
        replacement: [{
            match: /(\i\.\i)\.PRESET_VIDEO,(\i=)(\i\.\i)\.RESOLUTION_720,(\i=)(\i\.\i)\.FPS_30/,
            replace: "$1.PRESET_CUSTOM,$2$3.RESOLUTION_SOURCE,$4$5.FPS_60",
        }, {
            // Relay saves native quality before Discord registers its preview track.
            match: /(MEDIA_ENGINE_SET_GO_LIVE_SOURCE:function\(\i\)\{)/,
            replace: "$1if($self.captures())return!1;",
        }],
    }, {
        find: ".getDesktopSource(",
        replacement: {
            match: /(\(0,\i\.\i\)\(\{preset:\i\.\i\.PRESET_CUSTOM,resolution:\i\.height,frameRate:\i\.\i\.FPS_30,soundshareEnabled:!0\}\))(?=,\i\.\i\.getMediaEngine\(\)\.getDesktopSource)/,
            replace: "$self.captures()||$1",
        },
    }, {
        find: "ApplicationStreamPreviewUploadManager",
        replacement: { match: /removeAttribute\("srcObject"\)(?<=(\i)\..+?)/, replace: "pause(),$1.srcObject=null" },
    }, {
        find: "DISCONNECT_FROM_VOICE_CHANNEL]",
        replacement: { match: /\[\i\.\i\.DISCONNECT_FROM_VOICE_CHANNEL/,
            replace: '["RELAY_SCREEN_SHARE"]:{onTrigger(){$self.share(true)},keyEvents:{keyUp:!1,keyDown:!0}},$&' },
    }, {
        find: '"push-to-talk-priority"',
        replacement: { match: /=\[(\{id:.{0,25}value:\i\.\i\.UNASSIGNED)/,
            replace: '=[{id:"relay-screen-share",value:"RELAY_SCREEN_SHARE",label:"Relay Screenshare"},$1' },
    }],
    async share(instant = false) {
        if (!active) return;
        const existing = [...connections.values()].find(connection => connection.rtc.isStreamSender());
        if (existing) { stopStream(existing.rtc.getMediaStreamKey()); return; }
        const channelId = SelectedChannelStore.getVoiceChannelId();
        if (!channelId) return;
        const channel = ChannelStore.getChannel(channelId);
        const voice = [...connections.values()].find(connection => connection.rtc.context === "default" && connection.ready);
        if (!channel || !voice) return;
        if (sharing) return;
        sharing = true;
        try {
            const saved = getShareQuality();
            const engine = MediaEngineStore.getMediaEngine() as unknown as {
                getDesktopSource(quality: { height: number; frameRate: number }, audio: boolean): Promise<string>;
            };
            let source: Promise<string>;
            // The browser engine calls getDisplayMedia synchronously, then awaits capture.
            automaticCapture = instant;
            try { source = engine.getDesktopSource({ height: saved.height, frameRate: saved.fps }, saved.audio); }
            finally { automaticCapture = false; }
            const sourceId = await source;
            if (connections.get(voice.rtc) !== voice || !voice.ready || SelectedChannelStore.getVoiceChannelId() !== channelId) { endCapture(); return; }
            const quality = getShareQuality();
            startStream(channel.guild_id ?? null, channel.id, { pid: null, sourceId, sourceName: "Relay",
                sound: quality.audio, previewDisabled: !quality.preview });
        } catch (error) {
            if (!(error instanceof Error) || !["NotAllowedError", "AbortError"].includes(error.name)) toast("Relay: " + String(error));
        } finally { sharing = false; }
    },
    toolboxActions: { "Share Screen": () => plugin.share() },
    videoCodec(rtp: { codec: string; payload: number }[], fmtp: { payload: number; config: string }[], name: string) {
        return rtp.find(codec => codec.codec === name && (name !== "H264" || fmtp.some(format =>
            format.payload === codec.payload && /(?:^|;)profile-level-id=42e0[\da-f]{2}(?:;|$)/i.test(format.config)
                && /(?:^|;)packetization-mode=1(?:;|$)/.test(format.config))))
            ?? rtp.find(codec => codec.codec === name);
    },
    videoAnswer(answer: RTCSessionDescriptionInit, offer?: RTCSessionDescriptionInit): RTCSessionDescriptionInit {
        if (answer.type !== "answer" || !answer.sdp || !offer?.sdp) return answer;
        const offered = offer.sdp.split(/(?=^m=)/m);
        const sdp = answer.sdp.split(/(?=^m=)/m).map(section => {
            if (!section.startsWith("m=video ") || /a=rtpmap:\d+ AV1\//i.test(section)) return section;
            const mid = /^a=mid:(.+)\r?$/m.exec(section)?.[1];
            const source = offered.find(part => part.startsWith("m=video ") && /^a=mid:(.+)\r?$/m.exec(part)?.[1] === mid);
            const payload = source && /^a=rtpmap:(\d+) AV1\/90000\r?$/im.exec(source)?.[1];
            if (!payload) return section;
            const repair = new RegExp(`^a=fmtp:(\\d+) apt=${payload}(?:;[^\\r\\n]*)?\\r?$`, "m").exec(source)?.[1];
            const payloads = repair ? [payload, repair] : [payload];
            const lines = source.split(/\r?\n/).filter(line => payloads.some(pt =>
                line.startsWith(`a=rtpmap:${pt} `) || line.startsWith(`a=fmtp:${pt} `) || line.startsWith(`a=rtcp-fb:${pt} `)));
            return section.replace(/^(m=video [^\r\n]+)/, `$1 ${payloads.join(" ")}`)
                .replace(/(?:\r?\n)*$/, "\r\n") + lines.join("\r\n") + "\r\n";
        }).join("");
        return { type: answer.type, sdp };
    },
    encryptionWorker(url: URL, options: WorkerOptions) {
        return new Worker(blob(`const relayWorkerURL=${JSON.stringify(url.href)};\n${videoWorkerSource}`), options);
    },
    usesNativeVoice() { return active && !!registry; },
    captures() { return this.usesNativeVoice() && [...connections.values()].some(connection => !connection.owner && connection.ready); },
    handles(rtc: RTC) { return this.usesNativeVoice() && (rtc.context === "default" || rtc.isStreamSender()); },
    owns(rtc: RTC) { return connections.has(rtc); },
    connect(rtc: RTC, endpoint: string | null, token: string | null) {
        this.disconnect(rtc);
        if (rtc._destroyed) return;
        rtc._cancelReconnect();
        rtc._cleanupSocket();
        if (rtc._nextChannelId) { rtc._channelId = rtc._nextChannelId; rtc._nextChannelId = undefined; }
        rtc.endpoint = endpoint;
        rtc.token = token;
        if (!endpoint || !token) { rtc.setState("AWAITING_ENDPOINT"); return; }
        const owner = rtc.context === "stream" ? rtc.getExtraConnectionOptions().streamUserId : undefined;
        const connection: Connection = { id: crypto.randomUUID(), rtc, owner, ready: false, started: false };
        connections.set(rtc, connection);
        rtc._mediaEngineConnectionId = connection.id;
        rtc.setState("RTC_CONNECTING");
        ensurePlayback().then(current => {
            if (!active || connections.get(rtc) !== connection || playback !== current) return;
            send({ op: "connect", id: connection.id, user: rtc.userId, session: rtc.sessionId,
                server: rtc.trueServerId, channel: rtc.trueChannelId, endpoint: endpoint.replace(/^wss?:\/\//, "").replace(/\/$/, ""), token,
                sending: rtc.isStreamSender(), ...voiceState() });
            syncVoice();
        }).catch(error => {
            if (active && connections.get(rtc) === connection) fail(error.message);
        });
    },
    disconnect(rtc: RTC) {
        const connection = connections.get(rtc);
        if (!connection) return;
        send({ op: "stop", id: connection.id });
        connections.delete(rtc);
        if (rtc.isStreamSender()) endCapture();
        if (connection.owner) {
            playback?.removeScreen(connection.owner);
            const screen = screens.get(connection.owner);
            if (screen) { registry.remove(screen.id); screen.stream.getTracks().forEach(t => t.stop()); screens.delete(connection.owner); }
        }
        if (!connection.owner) {
            for (const child of [...connections.values()]) this.disconnect(child.rtc);
        }
        if (!connections.size) closePlayback();
    },
    start() {
        active = true;
        const module = find(m => typeof m === "object" && Object.values(m).some(v => typeof v === "function" && v.toString().includes('"VideoStream"')));
        if (module) {
            const functions = Object.values(module).filter(v => typeof v === "function") as Function[];
            registry = {
                add: functions.find(f => f.toString().includes('"VideoStream"')) as (stream: MediaStream) => string,
                remove: functions.find(f => f.toString().includes("delete ")) as (id: string) => void,
            };
        }
        if (!registry?.add || !registry.remove) { active = false; toast("Relay: Discord's video interface changed. Native media is disabled."); return; }
        for (const store of [MediaEngineStore, VoiceStateStore, SelectedChannelStore]) store.addChangeListener(syncVoice);
        originalCapture = navigator.mediaDevices.getDisplayMedia;
        navigator.mediaDevices.getDisplayMedia = nativeCapture;
        MediaEngineStore.emitChange();
    },
    stop() {
        active = false;
        MediaEngineStore.emitChange();
        closeShareDialog();
        if ([...connections.values()].some(c => !c.owner)) ChannelActions.selectVoiceChannel(null);
        for (const store of [MediaEngineStore, VoiceStateStore, SelectedChannelStore]) store.removeChangeListener(syncVoice);
        if (navigator.mediaDevices.getDisplayMedia === nativeCapture) navigator.mediaDevices.getDisplayMedia = originalCapture;
        preparing?.reject(new Error("Relay stopped"));
        preparing = undefined;
        captureTrack?.stop();
        for (const connection of connections.values()) connection.rtc.setState("RTC_DISCONNECTED");
        connections.clear();
        closePlayback();
    },
});

export default plugin;
