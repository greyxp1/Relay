// Receive media independently of rendering and layout on the UI thread.
const bridge = globalThis as unknown as Worker
let socket: WebSocket
bridge.onmessage = ({ data }) => {
  if (!data.url) {
    if (socket?.readyState === WebSocket.OPEN) socket.send(data)
    return
  }
  socket = new WebSocket(data.url)
  socket.binaryType = 'arraybuffer'
  socket.onmessage = ({ data }) => {
    if (data instanceof ArrayBuffer) bridge.postMessage(data, [data])
    else bridge.postMessage(data)
  }
  socket.onerror = socket.onclose = () => bridge.postMessage({ closed: true })
}
export {}
