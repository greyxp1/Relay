declare abstract class AudioWorkletProcessor {
  readonly port: MessagePort
}
declare function registerProcessor(name: string, processor: typeof AudioWorkletProcessor): void

class StreamAudio extends AudioWorkletProcessor {
  private samples = new Float32Array(1024)
  private offset = 0
  private output?: MessagePort

  constructor() {
    super()
    this.port.onmessage = ({ data }: MessageEvent<MessagePort>) => { this.output = data }
  }

  process([input]: Float32Array[][]) {
    const [left, right = left] = input
    if (!left || !this.output) {
      this.offset = 0
      return true
    }
    for (let frame = 0; frame < left.length; frame++) {
      this.samples[this.offset++] = left[frame]
      this.samples[this.offset++] = right[frame]
      if (this.offset === this.samples.length) {
        this.output.postMessage(this.samples.buffer, [this.samples.buffer])
        this.samples = new Float32Array(1024)
        this.offset = 0
      }
    }
    return true
  }
}

registerProcessor('relay-stream-audio', StreamAudio)
