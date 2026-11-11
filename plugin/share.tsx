import "./share.css";

import { FormSwitch } from "@components/FormSwitch";
import { ImageIcon, ScreenshareIcon } from "@components/Icons";
import { Text } from "@components/Text";
import { getUserSettingLazy } from "@api/UserSettings";
import { IS_WINDOWS } from "@utils/constants";
import { findByCodeLazy, findByPropsLazy } from "@webpack";
import { ApplicationStreamingSettingsStore, closeModal, Modal, openModal, React, Select, TextInput, useState, useStateFromStores } from "@webpack/common";
import type { RenderModalProps } from "@vencord/discord-types";

export type ShareQuality = { height: number; fps: number; audio: boolean; preview: boolean };
type SharePreferences = { source: string; autoShare: boolean };
type ShareOptions = ShareQuality & SharePreferences;
type VideoDevice = { id: string; name: string; screen?: boolean };

function useVideoDevices() {
    const [devices, setDevices] = useState<VideoDevice[]>();
    const [error, setError] = useState("");
    React.useEffect(() => {
        setDevices(undefined);
        setError("");
        const socket = new WebSocket("ws://127.0.0.1:38446/media");
        let received = false;
        const timeout = setTimeout(() => socket.close(), 5000);
        socket.onopen = () => socket.send(JSON.stringify({ op: "videoDevices" }));
        socket.onmessage = event => {
            const message = JSON.parse(event.data);
            if (message.op !== "videoDevices") return;
            received = true;
            if (message.error) setError(message.error);
            setDevices([...(message.screens ?? []).map(device => ({ ...device, screen: true })), ...(message.devices ?? [])]);
            socket.close();
        };
        socket.onclose = () => {
            clearTimeout(timeout);
            if (!received) setError("Could not load media sources. Check Relay and reopen this dialog.");
        };
        return () => { clearTimeout(timeout); socket.onclose = null; socket.onmessage = null; socket.onopen = null; socket.close(); };
    }, []);
    return { devices, error };
}
const updateStreamSettings = findByCodeLazy('type:"STREAM_UPDATE_SETTINGS"');
const streamPresets = findByPropsLazy("PRESET_CUSTOM");
const disablePreviews = getUserSettingLazy<boolean>("voiceAndVideo", "disableStreamPreviews");

export function getShareQuality(): ShareQuality {
    const saved = ApplicationStreamingSettingsStore.getState();
    return { height: saved.resolution, fps: saved.fps, audio: saved.soundshareEnabled, preview: !disablePreviews?.getSetting() };
}

export function saveShareQuality(change: Partial<ShareQuality>) {
    const quality = { ...getShareQuality(), ...change };
    if (change.preview !== undefined) disablePreviews?.updateSetting(() => !quality.preview);
    if ((change.height !== undefined || change.fps !== undefined || change.audio !== undefined) && validShareQuality(quality)) {
        updateStreamSettings({ preset: streamPresets.PRESET_CUSTOM, resolution: quality.height, frameRate: quality.fps, soundshareEnabled: quality.audio, noTrack: true });
    }
}

function validShareQuality(quality: ShareQuality) {
    return Number.isInteger(quality.height) && (quality.height === 0 || quality.height >= 16) && quality.height <= 2160
        && Number.isInteger(quality.fps) && quality.fps >= 1 && quality.fps <= 240;
}

export function RelaySettings({ settings }: { settings: { use(): SharePreferences; store: SharePreferences } }) {
    const { source, autoShare } = settings.use();
    const saved = useStateFromStores([ApplicationStreamingSettingsStore], () => ApplicationStreamingSettingsStore.getState());
    const preview = !disablePreviews?.useSetting();
    const discovery = useVideoDevices();
    return <ShareControls value={{ source, autoShare, height: saved.resolution, fps: saved.fps, audio: saved.soundshareEnabled, preview }}
        {...discovery} onChange={change => {
            saveShareQuality(change);
            if (change.source !== undefined) settings.store.source = change.source;
            if (change.autoShare !== undefined) settings.store.autoShare = change.autoShare;
        }} />;
}
const resolutions = [0, 480, 720, 1080, 1440, 2160];
const frameRates = [15, 30, 60, 120, 144, 240];
// Discord's field props are supported at runtime but missing from discord-types.
const QualitySelect = Select as React.ComponentType<Omit<React.ComponentProps<typeof Select>, "aria-label"> & {
    label?: string; "aria-label"?: string;
}>;

function QualityControl({ label, unit, value, minimum, maximum, presets, format, onChange }: {
    label: string; value: number; minimum: number; maximum: number; presets: number[];
    unit: string; format: (value: number) => string; onChange: (value: number) => void;
}) {
    const id = React.useId();
    const [custom, setCustom] = useState(!presets.includes(value));
    const [focusPresets, setFocusPresets] = useState(false);
    const [blurred, setBlurred] = useState(false);
    const [draft, setDraft] = useState(String(value || maximum));
    const number = Number(draft);
    const valid = draft.trim() !== "" && Number.isInteger(number) && number >= minimum && number <= maximum;
    const options = presets.map(preset => ({ value: preset, label: format(preset) }));
    function change(text: string) {
        setDraft(text);
        setBlurred(false);
        const number = Number(text);
        onChange(text.trim() !== "" && Number.isInteger(number) && number >= minimum && number <= maximum ? number : NaN);
    }
    return <div className="relay-quality">
        <Text id={id} size="md" weight="medium" color="text-strong">{custom ? `${label} (${unit})` : label}</Text>
        {custom ? <div className="relay-custom">
            <TextInput aria-labelledby={id} autoFocus onBlur={() => setBlurred(true)}
                placeholder={`${minimum}–${maximum}`}
                type="number" step="1" min={minimum} max={maximum} value={draft} onChange={change}
                error={blurred && !valid ? `Enter a whole number from ${minimum} to ${maximum.toLocaleString()}.` : undefined} />
            <QualitySelect aria-label={`${label} presets`} placeholder="Presets" options={options}
                isSelected={() => false} serialize={String} closeOnSelect select={preset => {
                    setFocusPresets(true);
                    setCustom(false);
                    onChange(preset);
                }} />
        </div> : <QualitySelect aria-label={label} autoFocus={focusPresets}
            options={[...options, { value: "custom", label: "Custom" }]}
            isSelected={selected => selected === (presets.includes(value) ? value : "custom")} serialize={String} closeOnSelect
            select={selected => {
                setCustom(selected === "custom");
                setBlurred(false);
                if (selected === "custom") change(draft);
                else onChange(selected);
            }} />}
    </div>;
}

function ShareControls({ value, devices, error, onChange }: {
    value: ShareOptions; devices?: VideoDevice[]; error: string; onChange: (change: Partial<ShareOptions>) => void;
}) {
    const id = React.useId();
    const screen = devices?.find(device => device.id === value.source)?.screen;
    const missing = value.source && devices && !devices.some(device => device.id === value.source);
    const status = error || (missing ? "This source is unavailable. Choose another source."
        : !devices ? "Looking for media sources…"
        : value.source ? screen ? "Share this screen or window." : "Share this device as a stream."
        : IS_WINDOWS ? "Choose a screen, window, camera or capture card." : "Your system picker opens before sharing.");
    const Icon = value.source && !screen ? ImageIcon : ScreenshareIcon;
    return <div className="relay-form">
        <div className="relay-source">
            <span className="relay-source-icon" aria-hidden="true"><Icon width={22} height={22} /></span>
            <div className="relay-source-control">
                <QualitySelect label="Source" placeholder="Choose a source" aria-describedby={id} options={[
                    ...(!IS_WINDOWS ? [{ value: "", label: "Screen or window…" }] : []),
                    ...(value.source && !devices?.some(device => device.id === value.source)
                        ? [{ value: value.source, label: `${devices ? "Unavailable source" : "Saved source"} (${value.source})` }] : []),
                    ...(devices?.map(device => ({ value: device.id, label: device.name })) ?? []),
                ]} select={source => onChange({ source })} isSelected={source => source === value.source} serialize={String} closeOnSelect />
                <p id={id} className={error || missing ? "relay-error" : "relay-status"} role="status">{status}</p>
            </div>
        </div>
        <div className="relay-quality-row">
            <QualityControl label="Resolution" unit="px"
                value={value.height} minimum={16} maximum={2160} presets={resolutions}
                format={height => height === 0 ? "Source" : `${height}p`} onChange={height => onChange({ height })} />
            <QualityControl label="Frame rate" unit="fps"
                value={value.fps} minimum={1} maximum={240} presets={frameRates}
                format={fps => `${fps} fps`} onChange={fps => onChange({ fps })} />
        </div>
        <div className="relay-options">
            <FormSwitch title="Share app audio" hideBorder value={value.audio} onChange={audio => onChange({ audio })} />
            <FormSwitch title="Show preview" hideBorder value={value.preview} onChange={preview => onChange({ preview })} />
        </div>
        <div className="relay-auto-share">
            <FormSwitch title="Share automatically on join" hideBorder
                description={value.source ? "Start this source with these quality settings." : IS_WINDOWS ? "Choose a source above to share when you join voice." : "Open the screen/window picker when you join voice."}
                value={value.autoShare} onChange={autoShare => onChange({ autoShare })} />
        </div>
    </div>;
}

function ShareDialog({ initial, changing, submit, ...props }: RenderModalProps & {
    initial: ShareOptions; changing: boolean; submit: (value: ShareOptions) => void;
}) {
    const [value, setValue] = useState(initial);
    const discovery = useVideoDevices();
    const valid = validShareQuality(value) && (value.source ? discovery.devices?.some(device => device.id === value.source) : !IS_WINDOWS);
    return <Modal {...props} title={changing ? "Change stream" : "Share your screen"}
        subtitle="Choose a source and your stream quality." size="lg"
        actions={[{ text: !IS_WINDOWS && !value.source ? "Choose screen or window" : changing ? "Apply changes" : "Start sharing", variant: "primary", disabled: !valid,
            onClick: () => { submit(value); props.onClose(); } }]}>
        <ShareControls value={value} {...discovery} onChange={change => setValue(value => ({ ...value, ...change }))} />
    </Modal>;
}

let pending: (() => void) | undefined;
export function selectShare(initial: ShareOptions, changing: boolean): Promise<ShareOptions> {
    if (pending) return Promise.reject(new DOMException("Sharing settings are already open", "InvalidStateError"));
    return new Promise((resolve, reject) => {
        const key = openModal(props => <ShareDialog {...props} initial={initial} changing={changing} submit={resolve} />, {
            onCloseRequest: () => closeModal(key),
            onCloseCallback: () => { pending = undefined; reject(new DOMException("Screen sharing cancelled", "NotAllowedError")); },
        });
        pending = () => closeModal(key);
    });
}
export function closeShareDialog() { pending?.(); }
