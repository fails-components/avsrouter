import {
  serialize as BSONserialize,
  deserialize as BSONdeserialize
} from 'bson'

class ParseHelper {
  constructor() {
    this.outpaketpos = 0
    this.outputqueue = []
    this.savedpaket = null
  }

  hasMessageOrPaket() {
    return this.outputqueue.length > 0
  }

  getMessageOrPaket() {
    if (this.outputqueue.length === 0)
      throw new Error('no Message or Paket available')
    return this.outputqueue.shift()
  }

  addPaket(paket) {
    let workpaket

    // merge with saved paket
    if (this.savedpaket) {
      const reschunk = new ArrayBuffer(
        this.savedpaket.byteLength + paket.byteLength
      )
      const srca = new Uint8Array(
        this.savedpaket.buffer,
        this.savedpaket.byteOffset,
        this.savedpaket.byteLength - this.readpos
      )
      workpaket = new Uint8Array(reschunk)
      for (let i = 0; i < this.savedpaket.length; i++) {
        workpaket[i] = this.savedpaket[i]
      }
      const dest2 = new Uint8Array(reschunk, this.savedpaket.length)
      for (let i = 0; i < paket.length; i++) {
        dest2[i] = paket[i]
      }
      this.savedpaket = null
    } else {
      workpaket = paket
    }

    let paketpos = 0
    let wdv = new DataView(
      workpaket.buffer,
      workpaket.byteOffset,
      workpaket.byteLength
    )

    while (paketpos < workpaket.byteLength) {
      if (this.outpaketpos === 0) {
        if (workpaket.byteLength - paketpos < 6) {
          this.savedpaket = new Uint8Array(
            workpaket.buffer,
            workpaket.byteOffset + paketpos,
            workpaket.byteLength - paketpos
          )
          paketpos = workpaket.byteLength
        } else {
          let payloadlen = wdv.getUint32(paketpos + 0)
          let hdrlen = wdv.getUint16(paketpos + 4)
          let bsonlen = 0
          if (hdrlen === 0) {
            bsonlen = payloadlen - 6
            payloadlen = 0
            hdrlen = 6
          }
          if (workpaket.byteLength - paketpos < hdrlen + bsonlen) {
            this.savedpaket = new Uint8Array(
              workpaket.buffer,
              workpaket.byteOffset + paketpos,
              workpaket.byteLength - paketpos
            )
            paketpos = workpaket.byteLength
          } else {
            if (bsonlen > 0) {
              this.outputqueue.push(
                BSONdeserialize(
                  new Uint8Array(
                    workpaket.buffer,
                    workpaket.byteOffset + paketpos + hdrlen,
                    bsonlen
                  )
                )
              )
              paketpos += hdrlen + bsonlen
            } else {
              let keyframe = false
              if ( hdrlen >= 17)
              {
                const hdrflags = wdv.getUint8(paketpos + 16)
                keyframe = !!(hdrflags & 1)
              }
              const towrite = Math.min(
                payloadlen + hdrlen,
                workpaket.byteLength - paketpos
              )
              this.outputqueue.push({
                paketstart: true,
                paket: new Uint8Array(
                  workpaket.buffer,
                  workpaket.byteOffset + paketpos,
                  towrite
                ),
                keyframe
              })
              if (towrite < payloadlen + hdrlen) {
                this.outpaketpos = towrite
                this.outpaketlen = payloadlen + hdrlen
              }
              paketpos += towrite
            }
          }
        }
      } else {
        const towrite = Math.min(
          this.outpaketlen - this.outpaketpos,
          workpaket.byteLength - paketpos
        )
        this.outputqueue.push({
          paketstart: false,
          paket: new Uint8Array(
            workpaket.buffer,
            workpaket.byteOffset + paketpos,
            towrite
          )
        })
        if (towrite < this.outpaketlen - this.outpaketpos) {
          this.outpaketpos += towrite
        } else {
          this.outpaketpos = 0
          this.outpaketlen = 0
        }
        paketpos += towrite
      }
    }
  }
}

export class AVSrouter {
  constructor(args) {
    this.realms = {}
  }

  getRealmObj(id, type) {
    if (!this.realms[id]) {
      this.realms[id] = {
        audio: {
          listeners: new Set(),
          source: null,
          messages: {}
        },
        video: {
          listeners: new Set(),
          source: null,
          messages: {}
        },
        screen: {
          listeners: new Set(),
          source: null,
          messages: {}
        }
      }
    }
    return this.realms[id][type]
  }

  getPaketCommiter(id, type) {
    const realm = this.getRealmObj(id, type)
    return (paket) => {
      const listeners = realm.listeners
      listeners.forEach((wc) => {
        wc(paket)
      })
    }
  }

  sendBson(tosend, sendmeth) {
    const bson = BSONserialize(tosend)
    const hdrlen = 6
    const headerbuffer = new ArrayBuffer(hdrlen)
    const hdrdv = new DataView(headerbuffer)
    let hdrpos = 0
    hdrdv.setUint32(hdrpos, bson.length + 6)
    hdrpos += 4
    hdrdv.setUint16(hdrpos, 0)
    sendmeth({message: new Uint8Array(headerbuffer)})
    sendmeth({message: bson })
  }

  getCommitAndStoreMessage(id, type) {
    return (message) => {
      const realm = this.getRealmObj(id, type)
      realm.messages[message.task] = message
      const listeners = realm.listeners
      this.sendBson(message, (buf) => {
        listeners.forEach((wc) => {
          wc(buf)
        })
      })
    }
  }

  removePaketCommiter(id, type, source) {
    const realm = this.getRealmObj(id, type)
    if (realm.source === source) {
      realm.source = null
      this.cleanUpRealm(id)
    }
  }

  unregisterStream(id, type, listener) {
    const realm = this.getRealmObj(id, type)
    if (realm.listeners.has(listener)) {
      realm.listeners.delete(listener)
      this.cleanUpRealm(id)
    }
  }

  registerStream(id, type, listener) {
    const realm = this.getRealmObj(id, type)
    realm.listeners.add(listener)
    for (let mess in realm.messages)
    {
      this.sendBson(realm.messages[mess], listener)
    }
  }

  cleanUpRealm(id) {
    const realm = this.realms[id]
    if (
      realm.audio.listeners.size === 0 &&
      !realm.audio.source &&
      realm.video.listeners.size === 0 &&
      !realm.video.source &&
      realm.screen.listeners.size === 0 &&
      realm.screen.source
    )
      delete this.realms[id]
  }

  async runServerLoop(server) {
    try {
      const sessionStream = await server.sessionStream('/avfails')
      const sessionReader = sessionStream.getReader()

      while (true) {
        const { done, value } = await sessionReader.read()
        if (done) {
          console.log('Server is exited')
          break
        }
        console.log('new session on avsrouter')
        this.handleSession(value)
      }
    } catch (error) {
      console.log('problem in runServerLoop', error)
    }
  }

  async handleSession(session) {
    let running = true
    await session.ready
    console.log('session is ready')
    session.closed.finally((reason) => {
      console.log('server session was closed', reason)
    })
    let authorized = false // we are not yet authorized

    // process the type incoming stream
    const processIncomingStream = async (args) => {
      const streamreader = args.streamReader
      const stream = args.stream
      const id = args.id
      const type = args.type
      const parseHelper = args.parseHelper

      const paketcommitter = this.getPaketCommiter(id, type)
      const commitAndStoreMessage = this.getCommitAndStoreMessage(id, type)
      let streamwriter
      try {
        streamwriter = await stream.writable.getWriter()

        while (running) {
          // first the paket, as they are processed partially
          while (parseHelper.hasMessageOrPaket()) {
            const chunk = parseHelper.getMessageOrPaket()
            if (chunk.paket)
              // maybe also test if arraybuffer
              paketcommitter(chunk)
            else {
              let store = false
              if (chunk.task && chunk.task === 'decoderconfig'){
                store = true
                console.log('decoderconfig', chunk)

              }
                
              if (store) commitAndStoreMessage(chunk)
            }
          }
          const readres = await streamreader.read()

          if (readres.value) parseHelper.addPaket(readres.value)
          if (readres.done) break
        }
      } catch (error) {
        console.log('error processIncomingStream', error)
      }
      try {
        this.removePaketCommiter(id, type, paketcommitter)
        streamreader.releaseLock()
        if (streamwriter) streamwriter.releaseLock()
        stream.readable.cancel()
        stream.writable.close()
      } catch (error) {
        console.log('error cleanup processIncomingStream', error)
      }
    }
    const processOutgoingStream = async (args) => {
      const streamreader = args.streamReader
      const stream = args.stream
      const id = args.id
      const type = args.type
      const parseHelper = args.parseHelper
      const streamwriter = await stream.writable.getWriter()
      let writeChunk
      try {
        // writing code
        let waitpaketstart = 1
        writeChunk = (chunk) => {
          if (chunk.paket)
          {
            if (waitpaketstart) {
              if (chunk.paketstart && chunk.keyframe)
                waitpaketstart = 0 // we need to start with complete pakets and a keyframe!
              else return
            }
            streamwriter.write(chunk.paket)
          } else if (chunk.message) { // message are always passed, no delay there
            streamwriter.write(chunk.message)
          }
        }
        this.registerStream(id, type, writeChunk)
        while (running) {
          while (parseHelper.hasMessageOrPaket()) {
            // only messages for controlling
            const message = parseHelper.getMessageOrPaket() // process them, e.g. change quality of stream
            // TODO implement
          }
          const readres = await streamreader.read()

          if (readres.value) parseHelper.addPaket(readres.value)
          if (readres.done) break
        }
      } catch (error) {
        console.log('error processOutgoingStream', error)
      }
      try {
        this.unregisterStream(id, type, writeChunk)

        streamreader.releaseLock()
        streamwriter.releaseLock()
        stream.readable.cancel()
        stream.writable.close()
      } catch (error) {
        console.log('error cleanup processIncomingStream', error)
      }
    }
    // our stream processor
    const processStream = async (stream) => {
      try {
        const streamReader = await stream.readable.getReader()
        const parseHelper = new ParseHelper()
        while (running) {
          const paket = await streamReader.read()
          if (paket.value) parseHelper.addPaket(paket.value)
          if (paket.done) break
          if (parseHelper.hasMessageOrPaket()) {
            const message = parseHelper.getMessageOrPaket()
            if (
              message.command == 'configure' &&
              (message.dir === 'incoming' || message.dir === 'outgoing') &&
              message.id &&
              (message.type === 'video' ||
                message.type === 'audio' ||
                message.type === 'screen')
            ) {
              // later may be routing code
              if (message.dir === 'incoming') {
                processIncomingStream({
                  stream,
                  streamReader,
                  parseHelper,
                  id: message.id,
                  type: message.type
                })
                break
              } else if (message.dir === 'outgoing') {
                processOutgoingStream({
                  stream,
                  streamReader,
                  parseHelper,
                  id: message.id,
                  type: message.type
                })
                break
              }
            } else {
              console.log('first message', message)
              throw new Error(
                'First message needs to be a configure either incoming or outgoing'
              )
            }
          }
        }
      } catch (error) {
        console.log('error in processStream', error)
      }
    }

    // now, we process every incoming bidistream and see what it wants
    try {
      const bidiReader = session.incomingBidirectionalStreams.getReader()
      while (running) {
        const bidistr = await bidiReader.read()
        if (bidistr.done) {
          console.log('bidiReader terminated')
          break
        }
        if (bidistr.value) {
          console.log('incoming bidirectional stream')
          processStream(bidistr.value)
        }
      }
    } catch (error) {
      console.log('bidirectional reader exited with', error)
    }
  }
}
