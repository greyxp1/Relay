// Capture uses 8-bit NV12. Read its sequence header so WebCodecs gets the negotiated profile and level.
function streamVideoCodec(data: Uint8Array, kind: number) {
  if (kind === 2) {
    for (let offset = 0; offset < data.length;) {
      const header = data[offset++], type = (header >> 3) & 15
      if (header & 4) offset++
      let size = data.length - offset
      if (header & 2) {
        size = 0
        let shift = 0, byte
        do {
          if (offset >= data.length || shift > 49) throw new Error('Invalid AV1 OBU size.')
          byte = data[offset++]
          size += (byte & 127) * 2 ** shift
          shift += 7
        } while (byte & 128)
      }
      const end = offset + size
      if (end > data.length) throw new Error('Truncated AV1 OBU.')
      if (type === 1) {
        let bit = offset * 8
        const read = count => {
          if (bit + count > end * 8) throw new Error('Truncated AV1 sequence header.')
          let value = 0
          while (count--) { value = value * 2 + ((data[bit >> 3] >> (7 - (bit & 7))) & 1); bit++ }
          return value
        }
        const profile = read(3)
        read(1)
        const reduced = read(1)
        if (!reduced) {
          if (read(1)) {
            read(32); read(32)
            if (read(1)) {
              let zeros = 0
              while (!read(1)) if (++zeros > 32) throw new Error('Invalid AV1 timing information.')
              read(zeros)
            }
            if (read(1)) { read(5); read(32); read(5); read(5) }
          }
          read(1); read(5); read(12)
        }
        const level = read(5), tier = !reduced && level > 7 && read(1) ? 'H' : 'M'
        return `av01.${profile}.${String(level).padStart(2, '0')}${tier}.08`
      }
      offset = end
    }
  } else {
    for (let i = 0; i + 7 < data.length; i++) {
      if (data[i] || data[i + 1] || data[i + 2] !== 1) continue
      if (kind === 0 && (data[i + 3] & 31) === 7)
        return 'avc1.' + [...data.subarray(i + 4, i + 7)].map(byte => byte.toString(16).padStart(2, '0')).join('')
      if (kind === 1 && ((data[i + 3] >> 1) & 63) === 33) {
        const header = []
        for (let j = i + 5; j < data.length && header.length < 13; j++) {
          if (data[j] === 3 && data[j - 1] === 0 && data[j - 2] === 0) continue
          header.push(data[j])
        }
        if (header.length < 13) break
        let flags = 0, compatibility = 0
        for (const byte of header.slice(2, 6)) flags = (flags << 8) | byte
        for (let bit = 0; bit < 32; bit++) { compatibility = (compatibility << 1) | (flags & 1); flags >>>= 1 }
        const constraints = header.slice(6, 12)
        while (constraints.length && !constraints.at(-1)) constraints.pop()
        const space = ['', 'A', 'B', 'C'][header[1] >> 6]
        return `hev1.${space}${header[1] & 31}.${(compatibility >>> 0).toString(16)}.${header[1] & 32 ? 'H' : 'L'}${header[12]}` +
          constraints.map(byte => '.' + byte.toString(16)).join('')
      }
    }
  }
  throw new Error('The received keyframe has no video configuration.')
}

// Encoded preview frames arrive from the local companion.
interface PlaybackDecoder {
  last: number
  decoder: VideoDecoder
}
interface PlaybackScreen {
  stream: MediaStream
  writer: WritableStreamDefaultWriter<VideoFrame>
  started: boolean
  recovering?: boolean
}
interface PlaybackConnection {
  url: string
  worker: string
  control: (message: any) => void
  frame: (owner: string, stream: MediaStream) => void
}
declare const MediaStreamTrackGenerator: {
  new (options: { kind: 'video' }): MediaStreamTrack & { writable: WritableStream<VideoFrame> }
}
export class BrowserPlayback {
  screens = new Map<string, PlaybackScreen>()
  decoders = new Map<string, PlaybackDecoder>()
  failed = new Set<string>()
  closed = false
  socket: Worker
  ready: Promise<void>
  disconnected?: () => void
  constructor(public report: (message: string, owner?: string, recover?: boolean) => void, public connection: PlaybackConnection) {}
  async start() {
    if (this.closed) return
    const socket = this.socket = new Worker(this.connection.worker, { type: 'module' })
    this.ready = new Promise<void>((resolve, reject) => {
      let ready = false
      socket.onmessage = ({ data }) => {
        if (data.closed) { disconnected(); return }
        if (typeof data !== 'string') return this.packet(new Uint8Array(data))
        const reply = JSON.parse(data)
        if (reply.op) { this.connection.control(reply); return }
        if (!ready) {
          if (reply.error) reject(new Error(reply.error))
          else { ready = true; resolve() }
        } else if (reply.error) this.fail(new Error(reply.error))
      }
      const disconnected = () => {
        const error = new Error('The local media connection closed.')
        if (!ready) reject(error)
        else if (!this.closed) this.fail(error)
      }
      this.disconnected = disconnected
      socket.onerror = disconnected
    })
    socket.postMessage({ url: this.connection.url })
    await this.ready
  }
  packet(bytes: Uint8Array) {
    if (this.closed) return
    if (bytes.length < 18) return this.fail(new Error('Invalid local media frame.'))
    const owner = bytes[0] === 2 || bytes[0] === 5
      ? (bytes[0] === 5 ? 'camera:' : '') + new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength).getBigUint64(2).toString() : undefined
    if (bytes.length > 1048594) return this.fail(new Error('Invalid local media frame.'), owner)
    if (owner && this.failed.has(owner)) return
    if (owner && this.screens.get(owner)?.recovering) {
      if (!(bytes[1] & 1)) return
      this.reset(owner)
    }
    try {
      this.decodePacket(bytes)
    } catch (error) {
      this.fail(error, owner)
    }
  }
  decodePacket(bytes: Uint8Array) {
    const view = new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength)
    const user = (bytes[0] === 5 ? 'camera:' : '') + view.getBigUint64(2).toString()
    if (bytes[0] === 4) { this.reset(user); return }
    if (bytes[0] !== 2 && bytes[0] !== 5) throw new Error('Invalid local media frame type.')
    const time = Number(view.getBigUint64(10)) / 4294967296
    const screen = this.screen(user)
    let entry = this.decoders.get(user)
    if (!entry) {
      if (!(bytes[1] & 1)) return
      if (this.decoders.size >= 128) throw new Error('Too many video streams.')
      if (!globalThis.VideoDecoder) throw new Error('This browser needs WebCodecs video decoding support to show the preview.')
      const codec = streamVideoCodec(bytes.subarray(18), bytes[1] >> 1)
      const active = () => !this.closed && this.decoders.get(user) === entry
      const decoder = new VideoDecoder({
        output: frame => {
          if (!active() || screen.writer.desiredSize <= 0) { frame.close(); return }
          screen.writer.write(frame).then(() => {
            if (active() && !screen.started) {
              screen.started = true
              this.connection.frame(user, screen.stream)
            }
          }).catch(error => { if (active()) this.fail(error, user) }).finally(() => frame.close())
        },
        error: error => { if (active()) { if (user.startsWith("camera:")) this.recover(user); else this.fail(error, user) } },
      })
      entry = { last: -1, decoder }
      this.decoders.set(user, entry)
      // Allow decoder buffering so software AV1 previews can keep up with high-FPS capture.
      decoder.configure({ codec })
    }
    if (time <= entry.last) return
    if (entry.decoder.decodeQueueSize >= 32) { this.recover(user); return }
    entry.last = time
    entry.decoder.decode(new EncodedVideoChunk({
      type: bytes[1] & 1 ? 'key' : 'delta', timestamp: Math.round(time * 1e6), data: bytes.subarray(18),
    }))
  }
  retire(id: string, entry: PlaybackDecoder) {
    if (entry.decoder.state !== 'closed') entry.decoder.close()
    this.decoders.delete(id)
  }
  screen(owner: string) {
    if (this.closed) throw new Error('The local media connection closed.')
    let screen = this.screens.get(owner)
    if (!screen) {
      let stream: MediaStream, writable: WritableStream<VideoFrame>
      if (globalThis.MediaStreamTrackGenerator) {
        const track = new MediaStreamTrackGenerator({ kind: 'video' })
        stream = new MediaStream([track])
        writable = track.writable
      } else {
        const canvas = document.createElement('canvas')
        stream = canvas.captureStream(0)
        const track = stream.getVideoTracks()[0] as CanvasCaptureMediaStreamTrack
        writable = new WritableStream({ write(frame: VideoFrame) {
          if (canvas.width !== frame.displayWidth) canvas.width = frame.displayWidth
          if (canvas.height !== frame.displayHeight) canvas.height = frame.displayHeight
          canvas.getContext('2d', { alpha: false }).drawImage(frame, 0, 0)
          track.requestFrame()
        } })
      }
      screen = { stream, writer: writable.getWriter(), started: false }
      this.screens.set(owner, screen)
    }
    return screen
  }
  reset(owner?: string) {
    if (owner) this.failed.delete(owner)
    else this.failed.clear()
    for (const [id, entry] of this.decoders) {
      if (owner && id !== owner) continue
      this.retire(id, entry)
    }
    for (const [id, screen] of this.screens) {
      if (owner && id !== owner) continue
      screen.recovering = false
    }
  }
  removeScreen(owner: string) {
    this.reset(owner)
    const screen = this.screens.get(owner)
    screen?.stream.getTracks().forEach(track => track.stop())
    screen?.writer.abort().catch(() => {})
    this.screens.delete(owner)
  }
  recover(owner: string) {
    if (this.closed || this.screen(owner).recovering) return
    this.reset(owner)
    this.screen(owner).recovering = true
    this.report('', owner, true)
  }
  fail(error: Error, owner?: string) {
    if (this.closed || (owner && this.failed.has(owner))) return
    if (owner) {
      this.reset(owner)
      this.failed.add(owner)
    } else this.close()
    this.report(error.message || String(error), owner)
  }
  close() {
    if (this.closed) return
    this.closed = true
    this.socket?.terminate()
    this.disconnected?.()
    this.reset()
    for (const owner of this.screens.keys()) this.removeScreen(owner)
  }
}
