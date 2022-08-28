import {
  serialize as BSONserialize,
  deserialize as BSONdeserialize,
  Decimal128
} from 'bson'
import jwt from 'jsonwebtoken'
import axios from 'axios'

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
      /*
      const srca = new Uint8Array(
        this.savedpaket.buffer,
        this.savedpaket.byteOffset,
        this.savedpaket.byteLength - this.readpos
      )
      */
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
    const wdv = new DataView(
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
              if (hdrlen >= 17) {
                const hdrflags = wdv.getUint8(paketpos + 16)
                keyframe = !!(hdrflags & 1)
              }
              let timestamp
              if (hdrlen >= 16) {
                timestamp = wdv.getBigInt64(paketpos + 8)
              }
              const towrite = Math.min(
                payloadlen + hdrlen,
                workpaket.byteLength - paketpos
              )
              this.outputqueue.push({
                paketstart: true,
                paketend: false,
                paket: new Uint8Array(
                  workpaket.buffer,
                  workpaket.byteOffset + paketpos,
                  towrite
                ),
                paketremain: payloadlen + hdrlen - towrite,
                keyframe,
                timestamp
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
          paketend: this.outpaketlen === this.outpaketpos + towrite,
          paketremain: this.outpaketlen - this.outpaketpos - towrite,
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
    this.primaryRealms = []
    this.keys = []
    this.spki = args.spki
    this.sessionCount = 0
    this.setupDispatcher()
  }

  setupDispatcher() {
    if (!process.env.AVSCONFIG) throw new Error('no avsconfig')
    const config = process.env.AVSCONFIG.split('|')
    if (config.length < 3) throw new Error('wrong avsconfig')
    this.region = config[0]
    if (typeof this.region !== 'string' || this.region.length < 3)
      throw new Error('wrong avsconfig')
    if (typeof config[1] !== 'string' || config[1].length < 8)
      throw new Error('wrong avsconfig hmac')
    const hmac = Buffer.from(config[1], 'base64')
    try {
      this.dispatcher = new URL(config[2])
    } catch (error) {
      throw new Error('Wrong url for disptacher ' + error)
    }

    if (!process.env.AVSMAXCLIENTS) throw new Error('AVSMAXCLIENTS missing')
    if (!process.env.AVSMAXREALMS) throw new Error('AVSMAXREALMS missing')
    if (!process.env.AVSROUTERURL) throw new Error('AVSROUTERURL missing')
    if (!process.env.AVSROUTERWSURL) throw new Error('AVSROUTERWSURL missing')

    axios.defaults.baseURL = config[2]
    this.axiosConfig = () => {
      const token = jwt.sign({ region: this.region }, hmac, {
        expiresIn: '30s',
        keyid: this.region,
        algorithm: 'HS512'
      })
      const config = {}
      config.headers = { authorization: 'Bearer ' + token }
      return config
    }
    // we need to report the dispatcher on a regular interval
    let updateid
    const updateDispatch = async () => {
      if (updateid) {
        clearTimeout(updateid)
        updateid = null
      }
      try {
        // ok first we have to gather all information
        const clients = Object.keys(this.realms)
        const realmInfo = clients.map((el) => {
          const addr = el.split(':')
          const realm = addr[0]
          addr.shift()
          return { realm, client: el, subClient: addr }
        })
        // now we count
        const numRealms = new Set(realmInfo.map((el) => el.realm)).size

        await axios.put(
          '/router',
          {
            url: process.env.AVSROUTERURL,
            wsurl: process.env.AVSROUTERWSURL,
            spki: this.spki,
            numClients: this.sessionCount,
            maxClients: parseInt(process.env.AVSMAXCLIENTS, 10),
            numRealms,
            maxRealms: parseInt(process.env.AVSMAXREALMS, 10),
            clients,
            primaryRealms: this.primaryRealms // these are the realms where this router is primary for this region
            // all routing for this realm and region should go through this router
          },
          { ...this.axiosConfig() }
        )
      } catch (error) {
        console.log('problem updating dispatch info: ', error)
      }

      updateid = setTimeout(updateDispatch, 20 * 1000 + Math.random() * 5000)
    }
    updateDispatch()
  }

  async fetchKey(key) {
    delete this.keys[key] // delete, important for key expiry
    try {
      const response = await axios.get('/key', {
        ...this.axiosConfig(),
        params: { kid: key }
      })
      if (!response.data || !response.data.key)
        throw new Error('no key retrieved')
      this.keys[key] = {}
      this.keys[key].publicKey = response.data.key
      this.keys[key].fetch = Date.now()
    } catch (err) {
      console.log('Error fetchKey', err)
      throw new Error('no key obtained')
    }
  }

  async verifyToken(token) {
    const decoded = jwt.decode(token)
    console.log('decoded jwt', decoded)
    if (!decoded) throw new Error('Authentification Error')
    const keyid = decoded.kid
    const time = Date.now()
    if (!this.keys[keyid] || this.keys[keyid].fetched + 60 * 1000 * 10 < time) {
      await this.fetchKey(keyid)
    }
    // console.log("keys",keyid, this.type, this.keys[keyid]);
    if (!this.keys[keyid]) {
      console.log('unknown key abort', keyid, this.type, time)
      throw new Error('Authentification Error, unknown keyid ' + keyid)
    }
    try {
      const dectoken = await new Promise((resolve, reject) => {
        jwt.verify(
          token,
          this.keys[keyid].publicKey /* TODO */,
          { algorithms: ['ES512'] },
          (err, decoded) => {
            if (err) {
              reject(new Error('Authentification Error'))
            } else {
              console.log('authorize worked!')
              resolve(decoded)
            }
          }
        )
      })
      return dectoken
    } catch (error) {
      throw new Error(error)
    }
  }

  getRealmObj(id, type) {
    if (!this.realms[id]) {
      this.realms[id] = {
        audio: {
          listeners: new Set(),
          sources: new Set(),
          messages: {},
          qualities: {} // available qualties and last incoming paket
        },
        video: {
          listeners: new Set(),
          sources: new Set(),
          messages: {},
          qualities: {} // available qualties and last incoming paket
        },
        screen: {
          listeners: new Set(),
          sources: new Set(),
          messages: {},
          qualities: {} // available qualties and last incoming paket
        }
      }
    }
    return this.realms[id][type]
  }

  getFixQuality(id, type) {
    const realm = this.getRealmObj(id, type)
    return (curqual) => {
      const now = Date.now()
      if (now - realm.qualities[curqual] < 1000) return curqual
      // we prefer a higher quality or equal if in doubt
      let newqual = -1
      for (const qual in realm.qualities) {
        if (
          now - realm.qualities[qual] < 1000 &&
          qual >= curqual && // include = since we use it also for an id switch
          (qual < newqual || newqual === -1)
        ) {
          newqual = qual
        }
      }
      if (newqual !== -1) return newqual
      // a lower will also do
      for (const qual in realm.qualities) {
        if (
          now - realm.qualities[qual] < 1000 &&
          qual < curqual &&
          qual > newqual
        ) {
          newqual = qual
        }
      }
      if (newqual !== -1) return newqual
      return curqual
    }
  }

  getIncreaseQual(id, type) {
    const realm = this.getRealmObj(id, type)
    return (curqual) => {
      const now = Date.now()
      // we prefer a higher quality if in doubt
      let newqual = -1
      for (const qual in realm.qualities) {
        if (
          now - realm.qualities[qual] < 1000 &&
          qual > curqual &&
          (qual < newqual || newqual === -1)
        ) {
          newqual = qual
        }
      }
      if (newqual !== -1) curqual = newqual
      return curqual
    }
  }

  getDecreaseQual(id, type) {
    const realm = this.getRealmObj(id, type)
    return (curqual) => {
      const now = Date.now()
      let newqual = -1
      // we like a lower lower will also do
      for (const qual in realm.qualities) {
        if (
          now - realm.qualities[qual] < 1000 &&
          qual < curqual &&
          qual > newqual
        ) {
          newqual = qual
        }
      }
      if (newqual !== -1) return newqual
      return curqual
    }
  }

  getSuspendQuality(id, type, quality) {
    const realm = this.getRealmObj(id, type)
    return () => {
      realm.qualities[quality] = 0 // set time to zero
    }
  }

  getPaketCommiter(id, type, quality) {
    const realm = this.getRealmObj(id, type)
    const commi = (paket) => {
      const listeners = realm.listeners
      listeners.forEach((wc) => {
        wc(paket, id, quality)
      })
      realm.qualities[quality] = Date.now()
    }

    realm.sources.add(commi)
    return commi
  }

  async sendBson(tosend, sendmeth) {
    const bson = BSONserialize(tosend)
    const hdrlen = 6
    const headerbuffer = new ArrayBuffer(hdrlen)
    const hdrdv = new DataView(headerbuffer)
    let hdrpos = 0
    hdrdv.setUint32(hdrpos, bson.length + 6)
    hdrpos += 4
    hdrdv.setUint16(hdrpos, 0)
    const send1 = sendmeth({ message: new Uint8Array(headerbuffer) })
    const send2 = sendmeth({ message: bson })
    await Promise.all([send1, send2])
  }

  getCommitAndStoreMessage(id, type, quality) {
    return (message) => {
      const realm = this.getRealmObj(id, type)
      if (!realm.messages[quality]) realm.messages[quality] = {}
      realm.messages[quality][message.task] = message
      const listeners = realm.listeners
      this.sendBson(message, (buf) => {
        listeners.forEach((wc) => {
          wc(buf, id, quality)
        })
      })
    }
  }

  removePaketCommiter(id, type, source) {
    const realm = this.getRealmObj(id, type)
    if (realm.sources.has(source)) {
      realm.sources.delete(source)
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
  }

  getSendInitialMessages(id, type, sender) {
    const realm = this.getRealmObj(id, type)
    return async (quality) => {
      const messages = realm.messages[quality]
      if (messages) {
        const proms = Object.keys(messages).map(
          async (mess) => await this.sendBson(messages[mess], sender)
        )
        await Promise.all(proms)
      }
    }
  }

  cleanUpRealm(id) {
    const realm = this.realms[id]
    if (
      realm.audio.listeners.size === 0 &&
      !realm.audio.sources.size === 0 &&
      realm.video.listeners.size === 0 &&
      !realm.video.sources.size === 0 &&
      realm.screen.listeners.size === 0 &&
      !realm.screen.sources.size === 0
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
    if (this.sessionCount + 1 >= parseInt(process.env.AVSMAXCLIENTS, 10)) {
      try {
        session.close({ reason: 'authorization timeout', closeCode: 500 })
      } catch (error) {
        console.log('Error emergency session close', error)
      }
      return
    }
    this.sessionCount++
    try {
      await session.ready
    } catch (error) {
      this.sessionCount--
      console.log('error session ready')
      return
    }
    console.log('session is ready')
    session.closed.finally((reason) => {
      this.sessionCount--
      console.log('server session was closed', reason)
    })
    // const authorized = false // we are not yet authorized
    // get a bidi stream for authorizations, a Datagram will not be reliable
    const realmsReadable = []
    const realmsWritable = []
    try {
      let authstream = await session.createBidirectionalStream()
      const areader = await authstream.readable.getReader()
      authstream.writable.close(0)
      // now we read all available data till the end for the token
      const timeout = setTimeout(() => {
        console.log('authorization timeout')
        session.close({ reason: 'authorization timeout', closeCode: 500 })
      }, 10 * 1000) // one second max
      const readarr = []
      let asize = 0
      let csize = -1
      while (csize === -1 || asize < csize) {
        const readres = await areader.read()
        console.log('readres', asize, readres)

        if (readres.value) {
          if (csize === -1) {
            csize = new DataView(readres.value.buffer).getInt32(0, true)
            console.log('csize', csize)
          }
          asize += readres.value.byteLength
          if (asize > 100000) throw new Error('authtoken too large') // prevent denial of service attack before auth
          readarr.push(readres.value)
        }
        if (readres.done) break
      }
      areader.cancel()
      authstream = null
      clearTimeout(timeout)
      const cctoken = new Uint8Array(asize)
      readarr.reduce((previousValue, currentValue) => {
        cctoken.set(currentValue, previousValue)
        return previousValue + currentValue.byteLength
      }, 0)
      const jwttoken = BSONdeserialize(cctoken)
      console.log('peak jwttoken', jwttoken)
      const authtoken = await this.verifyToken(jwttoken.token)
      console.log('peak authtoken', authtoken)
      if (authtoken.realmsReadable)
        realmsReadable.push(
          ...authtoken.realmsReadable.map((el) => new RegExp(el))
        )
      if (authtoken.realmsWritable)
        realmsWritable.push(
          ...authtoken.realmsReadable.map((el) => new RegExp(el))
        )
    } catch (error) {
      console.log('authorization stream failed', error)
      try {
        await new Promise((resolve) => setInterval(resolve, 1000)) // slow down potential attackers
        session.close({ reason: 'authorization failed', closeCode: 500 })
      } catch (error) {
        console.log('auth failed session close failed', error)
      }
      return
    }

    // process the type incoming stream
    const processIncomingStream = async (args) => {
      const streamreader = args.streamReader
      const stream = args.stream
      const id = args.id
      const type = args.type
      const parseHelper = args.parseHelper
      const quality = args.quality

      const paketcommitter = this.getPaketCommiter(id, type, quality)
      const commitAndStoreMessage = this.getCommitAndStoreMessage(
        id,
        type,
        quality
      )
      const suspendQuality = this.getSuspendQuality(id, type, quality)

      let streamwriter

      const writeStat = async (chunk) => {
        try {
          if (chunk.message) {
            // message are always passed, no delay there
            await streamwriter.write(chunk.message)
          }
        } catch (error) {
          console.log('writeStat failed', error)
        }
      }

      const sendStatBson = async (chunk) => {
        await this.sendBson(chunk, writeStat)
      }

      let curpaketsize
      const paketstat = (paket) => {
        if (paket.paketstart) {
          curpaketsize = paket.paket.byteLength
          sendStatBson({
            task: 'start',
            time: Date.now(),
            timestamp: new Decimal128(paket.timestamp.toString())
          })
        } else if (paket.paketend) {
          curpaketsize += paket.paket.byteLength
          sendStatBson({
            task: 'end',
            time: Date.now(),
            size: curpaketsize
          })
          curpaketsize = 0
        } else {
          curpaketsize += paket.paket.byteLength
        }
      }

      try {
        streamwriter = await stream.writable.getWriter()

        // eslint-disable-next-line no-unmodified-loop-condition
        while (running) {
          // first the paket, as they are processed partially
          while (parseHelper.hasMessageOrPaket()) {
            const chunk = parseHelper.getMessageOrPaket()
            if (chunk.paket) {
              paketstat(chunk)
              // maybe also test if arraybuffer
              paketcommitter(chunk)
            } else {
              let store = false
              if (chunk.task && chunk.task === 'decoderconfig') {
                store = true
                console.log('decoderconfig', chunk)
              } else if (chunk.task && chunk.task === 'suspendQuality') {
                store = false
                suspendQuality()
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
        /* await stream.writable.close()
        console.log('mark prob 4')
        await stream.readable.cancel()
        console.log('mark prob 6') */ // not needed
      } catch (error) {
        console.log('error cleanup processIncomingStream', error)
      }
    }
    const processOutgoingStream = async (args) => {
      // TODO check AUTHENTIFICATION
      let streamreader = args.streamReader
      const stream = args.stream
      let curid = args.id
      let newid
      const type = args.type
      const parseHelper = args.parseHelper
      let curqual = -1
      let nextqual // tells us that we should change on the next keyframe
      let lastpaket = 0
      let streamwriter
      let writeChunk
      let fixQuality = this.getFixQuality(curid, type)
      let increaseQual = this.getIncreaseQual(curid, type)
      let decreaseQual = this.getDecreaseQual(curid, type)

      let qualchangeStor = []

      let outgoingbuffer = 0

      try {
        streamwriter = await stream.writable.getWriter()
        const sIMsend = async (paket) => {
          try {
            if (streamwriter) await streamwriter.write(paket.message)
          } catch (error) {
            console.log('error in sendInitialMessages 1', streamwriter)
          }
        }
        let sendInitialMessages = this.getSendInitialMessages(
          curid,
          type,
          sIMsend
        )
        // writing code
        let waitpaketstart = 1
        let writefailedres = null
        let inpaket = false
        let paketremain = 0
        const writefailed = new Promise((resolve) => {
          writefailedres = resolve
        })
        writeChunk = async (chunk, pid, quality) => {
          const now = Date.now()
          if (now - lastpaket > 1000) {
            // recheck quality
            const newqual = fixQuality(curqual)
            if (newqual !== curqual) {
              waitpaketstart = 1 // we need a key frame
              curqual = newqual
              console.log('new quality', curqual)
              if (paketremain > 0) {
                // we finish the package with garbage, intentionelle uninitalized
                const fakearray = new Uint8Array(new ArrayBuffer(paketremain))
                for (let i = 0; i < paketremain - 1; i++) fakearray[i] = 0
                fakearray[paketremain - 1] = 1
                streamwriter.write(fakearray)
                inpaket = false
                paketremain = 0
              }
              // if changed may be emit an information for client ? TODO
              sendInitialMessages(curqual) // no await!
            }
          }
          if (
            (nextqual || newid) &&
            quality === (nextqual || curqual) &&
            (pid === (newid || curid) || newid === 'sleep') &&
            ((chunk.paketstart && chunk.keyframe) || qualchangeStor.length > 0)
          ) {
            if (!inpaket) {
              // we can change
              if (nextqual) {
                curqual = nextqual
                nextqual = undefined
              }
              if (newid) {
                this.unregisterStream(curid, type, writeChunk)
                sendInitialMessages = this.getSendInitialMessages(
                  newid,
                  type,
                  sIMsend
                )
                fixQuality = this.getFixQuality(newid, type)
                increaseQual = this.getIncreaseQual(newid, type)
                decreaseQual = this.getDecreaseQual(newid, type)
                curid = newid
                newid = undefined
              }
              sendInitialMessages(curqual) // init the decoder // no await
              for (const el of qualchangeStor) {
                inpaket = true
                if (chunk.paketend) inpaket = false
                streamwriter.write(el.paket) // no await
              }
              qualchangeStor = []
            } else {
              if (newid !== 'sleep') qualchangeStor.push(chunk)
            }
          }
          // if we decrease quality we should only send the current paket and stop then
          if (nextqual && nextqual < curqual && !inpaket) return
          if (quality !== curqual || pid !== curid) return // not the subscribed quality or id
          // do not hold more than 1 MB in buffers
          if (!inpaket && outgoingbuffer > 1000000) return
          try {
            if (chunk.paket) {
              lastpaket = now
              if (waitpaketstart) {
                if (chunk.paketstart && chunk.keyframe)
                  waitpaketstart = 0 // we need to start with complete pakets and a keyframe!
                else return
              }
              inpaket = true
              if (chunk.paketend) inpaket = false
              if (chunk.paketremain) paketremain = chunk.paketremain
              else paketremain = 0

              outgoingbuffer += chunk.paket.byteLength
              await streamwriter.write(chunk.paket)
              outgoingbuffer -= chunk.paket.byteLength
            } else if (chunk.message) {
              // message are always passed, no delay there
              await streamwriter.write(chunk.message)
            }
          } catch (error) {
            if (writefailedres) writefailedres()
            running = false
          }
        }
        this.registerStream(curid, type, writeChunk)
        const reading = async () => {
          // eslint-disable-next-line no-unmodified-loop-condition
          while (running) {
            while (parseHelper.hasMessageOrPaket()) {
              // only messages for controlling
              const message = parseHelper.getMessageOrPaket() // process them, e.g. change quality of stream
              if (message.task === 'incQual') {
                const newqual = increaseQual(curqual)
                if (newqual !== curqual) {
                  nextqual = newqual
                  qualchangeStor = [] // reset any already ongoing change
                }
              } else if (message.task === 'decQual') {
                const newqual = decreaseQual(curqual)
                if (newqual !== curqual) {
                  nextqual = newqual
                  qualchangeStor = [] // reset any already ongoing change
                }
              } else if (message.task === 'chgId' && curid !== message.id) {
                console.log('incoming change', curid, message.id)

                if (message.id) {
                  console.log('change to', message.id)
                  // TODO check AUTHENTIFICATION
                  newid = message.id
                  const checkqual = fixQuality(curqual)
                  if (checkqual !== curqual) nextqual = checkqual

                  this.registerStream(newid, type, writeChunk)
                } else {
                  newid = 'sleep'
                }
                qualchangeStor = [] // reset any already ongoing change
              }
            }
            const readres = await streamreader.read()

            if (readres.value) parseHelper.addPaket(readres.value)
            if (readres.done) break
          }
        }
        await Promise.race([await reading(), writefailed])
      } catch (error) {
        console.log('error processOutgoingStream', error)
      }
      try {
        this.unregisterStream(curid, type, writeChunk)
        if (newid) this.unregisterStream(newid, type, writeChunk)

        streamreader.releaseLock()
        streamreader = undefined
        streamwriter.releaseLock()
        streamwriter = undefined
        /* await stream.readable.cancel()
        console.log('mark prob 10')
        await stream.writable.close()
        console.log('mark prob 11') */ // not needed
      } catch (error) {
        console.log('error cleanup processIncomingStream', error)
      }
    }
    // our stream processor
    const processStream = async (stream) => {
      try {
        const streamReader = await stream.readable.getReader()
        const parseHelper = new ParseHelper()
        // eslint-disable-next-line no-unmodified-loop-condition
        while (running) {
          const paket = await streamReader.read()
          if (paket.value) parseHelper.addPaket(paket.value)
          if (paket.done) break
          if (parseHelper.hasMessageOrPaket()) {
            const message = parseHelper.getMessageOrPaket()
            if (
              message.command === 'configure' &&
              (message.dir === 'incoming' || message.dir === 'outgoing') &&
              message.id &&
              (message.dir === 'outgoing' || message.quality) &&
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
                  type: message.type,
                  quality: message.quality
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
              console.log('first message ignore close', message)
              streamReader.releaseLock()
              stream.readable.cancel()
              break
            }
          }
          console.log('paket loop end')
        }
        console.log('processStream exited')
      } catch (error) {
        console.log('error in processStream', error)
      }
    }

    let bidicount = 0
    // now, we process every incoming bidistream and see what it wants
    try {
      const bidiReader = session.incomingBidirectionalStreams.getReader()
      // eslint-disable-next-line no-unmodified-loop-condition
      while (running) {
        const bidistr = await bidiReader.read()
        if (bidistr.done) {
          console.log('bidiReader terminated')
          break
        }
        if (bidistr.value) {
          bidicount++
          console.log('incoming bidirectional stream', bidicount)
          processStream(bidistr.value)
        }
      }
    } catch (error) {
      console.log('bidirectional reader exited with', error)
    }
  }
}
