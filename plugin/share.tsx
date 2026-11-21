import "./share.css";

import { FormSwitch } from "@components/FormSwitch";
import { Text } from "@components/Text";
import { getUserSettingLazy } from "@api/UserSettings";
import { findByCodeLazy, findByPropsLazy } from "@webpack";
import { ApplicationStreamingSettingsStore, closeModal, Modal, openModal, React, Select, TextInput, useState, useStateFromStores } from "@webpack/common";
import type { RenderModalProps } from "@vencord/discord-types";

export type ShareQuality = { height: number; fps: number; audio: boolean; preview: boolean };
type ShareOptions = ShareQuality & { autoShare: boolean };

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
    return Number.isInteger(quality.height) && (quality.height === 0 || quality.height >= 128) && quality.height <= 2160
        && Number.isInteger(quality.fps) && quality.fps >= 1 && quality.fps <= 240;
}

export function RelaySettings({ settings }: { settings: { use(): { autoShare: boolean }; store: { autoShare: boolean } } }) {
    const { autoShare } = settings.use();
    const saved = useStateFromStores([ApplicationStreamingSettingsStore], () => ApplicationStreamingSettingsStore.getState());
    const preview = !disablePreviews?.useSetting();
    return <ShareControls value={{ autoShare, height: saved.resolution, fps: saved.fps, audio: saved.soundshareEnabled, preview }}
        onChange={change => {
            saveShareQuality(change);
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

function ShareControls({ value, onChange }: { value: ShareOptions; onChange: (change: Partial<ShareOptions>) => void }) {
    return <div className="relay-form">
        <div className="relay-quality-row">
            <QualityControl label="Resolution" unit="px"
                value={value.height} minimum={128} maximum={2160} presets={resolutions}
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
                description="Open the screen/window picker when you join voice."
                value={value.autoShare} onChange={autoShare => onChange({ autoShare })} />
        </div>
    </div>;
}

function ShareDialog({ initial, changing, submit, ...props }: RenderModalProps & {
    initial: ShareOptions; changing: boolean; submit: (value: ShareOptions) => void;
}) {
    const [value, setValue] = useState(initial);
    return <Modal {...props} title={changing ? "Change stream" : "Share your screen"}
        subtitle="Choose your stream quality. Your system picker opens next." size="lg"
        actions={[{ text: "Choose screen or window", variant: "primary", disabled: !validShareQuality(value),
            onClick: () => { submit(value); props.onClose(); } }]}>
        <ShareControls value={value} onChange={change => setValue(value => ({ ...value, ...change }))} />
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
