declare const relayWorkerURL: string

// WebRTC restores the last OBU's size field, but DAVE authenticates it without one.
function restoreAV1(buffer: ArrayBuffer): ArrayBuffer {
  const bytes = new Uint8Array(buffer)
  if (bytes.at(-1) !== 0xfa || bytes.at(-2) !== 0xfa) return buffer
  let at = 0
  while (at < bytes.length) {
    const header = at, flags = bytes[at++]
    if (flags & 0x81 || !(flags & 2)) return buffer
    if (flags & 4) at++
    const sizeAt = at
    let size = 0, shift = 0, byte: number
    do {
      if (at >= bytes.length || shift > 28) return buffer
      byte = bytes[at++]
      size += (byte & 127) * 2 ** shift
      shift += 7
    } while (byte & 128)
    const payloadAt = at, end = at + size
    if (end > bytes.length) return buffer
    if (end === bytes.length) {
      const output = new Uint8Array(bytes.length - (payloadAt - sizeAt))
      output.set(bytes.subarray(0, sizeAt))
      output[header] &= ~2
      output.set(bytes.subarray(payloadAt), sizeAt)
      return output.buffer
    }
    at = end
  }
  return buffer
}

const prototype = (globalThis as any).RTCEncodedVideoFrame?.prototype
const data = prototype && Object.getOwnPropertyDescriptor(prototype, 'data')
if (data?.get && data.set) {
  Object.defineProperty(prototype, 'data', { ...data, get() {
    const buffer = data.get!.call(this)
    if (this.getMetadata().mimeType !== 'video/AV1') return buffer
    const restored = restoreAV1(buffer)
    if (restored !== buffer) data.set!.call(this, restored)
    return restored
  } })
}

// Keep Discord's asset URLs relative to its original worker rather than the blob.
const scope = globalThis as unknown as { importScripts(...urls: string[]): void }
const load = scope.importScripts.bind(scope)
scope.importScripts = (...urls) => load(...urls.map(url => new URL(url, relayWorkerURL).href))
Object.defineProperty(globalThis, 'location', { value: new URL(relayWorkerURL) })
load(relayWorkerURL)
