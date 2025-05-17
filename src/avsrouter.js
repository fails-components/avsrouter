import {
  serialize as BSONserialize,
  deserialize as BSONdeserialize,
  Decimal128
} from 'bson'
import jwt from 'jsonwebtoken'
import axios from 'axios'
import { webcrypto as crypto } from 'crypto'
import { WebTransport } from '@fails-components/webtransport'

export function glog() {
  const date = new Date().toLocaleString('en', { hour12: false })
  process.stdout.write('[' + date + '] ')
  console.log.apply(console, arguments)
  return this
}

export { glog as log }

class AsyncPaketPipe {
  constructor() {
    this.pakets = []
    this.waitpromres = []
    this.waitpromrej = []
    this.paketnum = 0
  }

  addPaket(paket) {
    paket.num = this.paketnum
    this.paketnum++
    const plength = this.pakets.length
    if (plength === 0 && this.waitpromres.length > 0) {
      this.waitpromrej.shift()
      const res = this.waitpromres.shift()
      res(paket)
    } else {
      this.pakets.push(paket)
    }
  }

  getPaket() {
    if (this.pakets.length > 0) {
      this.paketnum--
      return Promise.resolve(this.pakets.shift())
    } else {
      return new Promise((resolve, reject) => {
        this.waitpromres.push(resolve)
        this.waitpromrej.push(reject)
      })
    }
  }

  numPackets() {
    return this.paketnum
  }

  flush() {
    const rejects = this.waitpromrej
    this.waitpromres = []
    this.waitpromrej = []
    rejects.forEach((res) => res())
  }
}

class ParseHelper {
  constructor() {
    this.outpaketpos = 0
    this.outpaket = undefined
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
              let temporalLayerId = 0
              if (hdrlen >= 17) {
                const hdrflags = wdv.getUint8(paketpos + 16)
                keyframe = !!(hdrflags & 1)
                if (hdrflags & (1 << 1) && hdrlen >= 22) {
                  // for temporallayer id
                  temporalLayerId = wdv.getUint32(paketpos + 18)
                }
              }
              let timestamp
              if (hdrlen >= 16) {
                timestamp = wdv.getBigInt64(paketpos + 8)
              }
              const towrite = Math.min(
                payloadlen + hdrlen,
                workpaket.byteLength - paketpos
              )
              const outpaketbuf = new Uint8Array(payloadlen + hdrlen)
              this.outpaket = {
                paketstart: true,
                paketend: true,
                paket: outpaketbuf,
                paketremain: 0, // payloadlen + hdrlen - towrite,
                keyframe,
                temporalLayerId,
                timestamp
              }
              // copy data, zero copy is not an advantage, since the overhead for distributing to too big
              const byteoff = workpaket.byteOffset + paketpos
              for (let run = 0; run < towrite; run++) {
                outpaketbuf[run] = workpaket[byteoff + run]
              }
              if (towrite < payloadlen + hdrlen) {
                this.outpaketpos = towrite
                this.outpaketlen = payloadlen + hdrlen
              } else {
                this.outputqueue.push(this.outpaket)
                delete this.outpaket
                this.outpaketpos = 0
                this.outpaketlen = 0
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
        const outpaketbuf = this.outpaket.paket
        const byteoff = workpaket.byteOffset + paketpos
        const byteoffdest = this.outpaketpos

        for (let run = 0; run < towrite; run++) {
          outpaketbuf[byteoffdest + run] = workpaket[byteoff + run]
        }
        if (towrite < this.outpaketlen - this.outpaketpos) {
          this.outpaketpos += towrite
        } else {
          this.outpaketpos = 0
          this.outpaketlen = 0
          this.outpaket.incomtime = Date.now()
          this.outputqueue.push(this.outpaket)
          delete this.outpaket
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
    this.rservers = {}
    this.spki = args.spki
    this.port = args.port
    this.sessionCount = 0
    this.sessionCountRouterClients = 0 // does not count towards sessionCount
    this.sessionCountRouters = 0
    this.setupDispatcher()
  }

  async setupDispatcher() {
    if (!process.env.AVSCONFIG) throw new Error('no avsconfig')
    const config = process.env.AVSCONFIG.split('|')
    if (config.length < 3) throw new Error('wrong avsconfig')
    this.region = config[0]
    if (typeof this.region !== 'string' || this.region.length < 3)
      throw new Error('wrong avsconfig region')
    if (typeof config[1] !== 'string' || config[1].length < 8)
      throw new Error('wrong avsconfig hmac')
    const hmac = Buffer.from(config[1], 'base64')
    try {
      this.dispatcher = new URL(config[2])
    } catch (error) {
      throw new Error('Wrong url ' + config[2] + ' for dispatcher ' + error)
    }

    if (!process.env.AVSMAXCLIENTS) throw new Error('AVSMAXCLIENTS missing')
    if (!process.env.AVSMAXREALMS) throw new Error('AVSMAXREALMS missing')
    if (!process.env.AVSROUTERHOST) throw new Error('AVSROUTERHOST missing')

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
    try {
      this.keypair = crypto.subtle.generateKey(
        {
          name: 'RSA-OAEP',
          modulusLength: 4096,
          publicExponent: new Uint8Array([1, 0, 1]),
          hash: 'SHA-256'
        },
        true,
        ['encrypt', 'decrypt']
      )
      const keypair = await this.keypair
      this.keypublic = await crypto.subtle.exportKey('jwk', keypair.publicKey)
    } catch (error) {
      glog('problem generating privkey pair', error)
    }
    // we need to report the dispatcher on a regular interval
    let updateid
    this.updateDispatch = async () => {
      if (updateid) {
        clearTimeout(updateid)
        updateid = null
      }
      try {
        // but before clean up all realms
        Object.keys(this.realms).forEach((el) => this.cleanUpRealm(el))
        // ok first we have to gather all information
        // log('debug router realms', this.realms)
        const allclients = Object.keys(this.realms)
        const realmInfo = allclients.map((el) => {
          const addr = el.split(':')
          const realm = addr[0]
          addr.shift()

          return { realm, client: el, subClient: addr }
        })

        const localClients = allclients.filter((el) => {
          const realm = this.realms[el]
          return (
            realm.audio.localSources.size > 0 ||
            realm.video.localSources.size > 0 ||
            realm.screen.localSources.size > 0
          )
        })
        const remoteClients = allclients.filter((el) => {
          const realm = this.realms[el]
          return (
            realm.audio.remoteSources.size > 0 ||
            realm.video.remoteSources.size > 0 ||
            realm.screen.remoteSources.size > 0
          )
        })
        // now we count
        const numRealms = new Set(realmInfo.map((el) => el.realm)).size

        const primRealms = Object.entries(this.primaryRealms)
          .filter((el) => el[1] > 0)
          .map((el) => el[0])

        /* log('debug router info', {
          url: process.env.AVSROUTERURL,
          wsurl: process.env.AVSROUTERWSURL,
          spki: this.spki,
          numRouters: this.sessionCountRouters,
          numClients: this.sessionCount,
          numRouterClients: this.sessionCountRouterClients,
          maxClients: parseInt(process.env.AVSMAXCLIENTS, 10),
          numRealms,
          maxRealms: parseInt(process.env.AVSMAXREALMS, 10),
          key: JSON.stringify(this.keypublic), // check if stringify is necessary
          localClients,
          remoteClients,
          primaryRealms: primRealms // these are the realms where this router is primary for this region
          // all routing for this realm and region should go through this router
        }) */

        await axios.put(
          '/router',
          {
            url: process.env.AVSROUTERURL
              ? process.env.AVSROUTERURL
              : 'https://' +
                process.env.AVSROUTERHOST +
                (this.port === 443 ? '' : ':' + this.port) +
                '/avfails',
            wsurl: process.env.AVSROUTERWSURL
              ? process.env.AVSROUTERWSURL
              : 'wss://' +
                process.env.AVSROUTERHOST +
                (this.port === 443 ? '' : ':' + this.port) +
                '/avfails',
            spki: this.spki,
            numRouters: this.sessionCountRouters,
            numClients: this.sessionCount,
            numRouterClients: this.sessionCountRouterClients,
            maxClients: parseInt(process.env.AVSMAXCLIENTS, 10),
            numRealms,
            maxRealms: parseInt(process.env.AVSMAXREALMS, 10),
            key: JSON.stringify(this.keypublic), // check if stringify is necessary
            localClients,
            remoteClients,
            primaryRealms: primRealms // these are the realms where this router is primary for this region
            // all routing for this realm and region should go through this router
          },
          { ...this.axiosConfig() }
        )
      } catch (error) {
        glog('problem updating dispatch info: ', error)
      }

      updateid = setTimeout(
        this.updateDispatch,
        20 * 1000 + Math.random() * 5000
      )
    }
    this.updateDispatch()
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
      glog('Error fetchKey', err)
      throw new Error('no key obtained')
    }
  }

  async verifyToken(token) {
    const decoded = jwt.decode(token)
    if (!decoded) throw new Error('Authentification Error')
    const keyid = decoded.kid
    const time = Date.now()
    if (!this.keys[keyid] || this.keys[keyid].fetched + 60 * 1000 * 10 < time) {
      await this.fetchKey(keyid)
    }
    if (!this.keys[keyid]) {
      glog('unknown key abort', keyid, this.type, time)
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

  getRealmColl(id) {
    if (!this.realms[id]) {
      this.realms[id] = {
        audio: {
          listeners: new Set(),
          localSources: new Set(),
          remoteSources: new Set(),
          rsession: new Set(),
          messages: {},
          qualities: {} // available qualties and last incoming paket
        },
        video: {
          listeners: new Set(),
          localSources: new Set(),
          remoteSources: new Set(),
          rsession: new Set(),
          messages: {},
          qualities: {} // available qualties and last incoming paket
        },
        screen: {
          listeners: new Set(),
          localSources: new Set(),
          remoteSources: new Set(),
          rsession: new Set(),
          messages: {},
          qualities: {} // available qualties and last incoming paket
        },
        numClients: 0,
        numRclients: 0,
        numRouter: 0
      }
    }
    return this.realms[id]
  }

  getRealmObj(id, type) {
    return this.getRealmColl(id)[type]
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

  getPaketCommiter(id, type, quality, isRouter) {
    const realm = this.getRealmObj(id, type)
    const commi = (paket) => {
      const listeners = realm.listeners
      listeners.forEach((wc) => {
        wc(paket, id, quality)
      })
      realm.qualities[quality] = Date.now()
    }
    if (isRouter) realm.remoteSources.add(commi)
    else realm.localSources.add(commi)
    return commi
  }

  async sendBson(tosend, sendmeth) {
    try {
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
    } catch (error) {
      glog('problem send bson', tosend)
      throw new Error('sendBson failed:', error)
    }
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

  removePaketCommiter(id, type, source, isRouter) {
    const realm = this.getRealmObj(id, type)
    let hassource = false
    if (realm.localSources.has(source)) {
      realm.localSources.delete(source)
      if (isRouter) throw new Error('Router client commiter in localSources')
      hassource = true
    }
    if (realm.remoteSources.has(source)) {
      realm.remoteSources.delete(source)
      if (!isRouter) throw new Error('Local client commiter in remoteSources')
      hassource = true
    }
    if (hassource) this.cleanUpRealm(id)
    else
      throw new Error('Problem removePaketCommiter no remote or local Source')
  }

  async acquireStreams({ id, type, next, nextspki, tickets }) {
    const realm = this.getRealmObj(id, type)
    if (realm.localSources.size > 0 || realm.remoteSources.size > 0) return
    if (realm.rsession.size > 0) return
    // we do not have a local source or a remote session attached, we have to attach a new session
    let removed = false
    const myrsession = {
      remove: () => {
        removed = true
      }
    }
    realm.rsession.add(myrsession)
    try {
      const serv = await this.getRemoteServer({
        next,
        nextspki,
        remove: () => {
          realm.rsession.delete(myrsession)
        },
        id,
        type,
        tickets
      })
      if (!serv) throw new Error('Did not get remote server')
      if (removed) {
        serv.unRegister(id, type, tickets)
      }
      myrsession.remove = () => {
        console.trace()
        serv.unRegister(id, type, tickets)
        realm.rsession.delete(myrsession)
      }
    } catch (error) {
      throw new Error('Acquire Streams prob:' + error)
    }
  }

  async getRemoteServer({ next, nextspki, remove, id, type, tickets }) {
    if (this.rservers[next]) {
      const serv = this.rservers[next]
      serv.register(id, type, tickets, remove)
      return serv
    } else {
      try {
        const rserv = (this.rservers[next] = {
          realms: new Map(),
          requests: [],
          requestRes: []
        })
        rserv.requests.push(
          new Promise((resolve) => {
            rserv.requestRes.push(resolve)
          })
        )
        const addReq = (req) => {
          rserv.requests.push(
            new Promise((resolve) => {
              rserv.requestRes.push(resolve)
            })
          )
          // rserv.requests.shift()
          const res = rserv.requestRes.shift()
          res(req)
        }
        rserv.register = (id, type, tickets, remove) => {
          const realmkey = id + ':' + type
          if (!rserv.realms.has(realmkey)) {
            const forRemoval = new Set()
            forRemoval.add(remove)
            // already
            rserv.realms.set(realmkey, {
              remove: () => forRemoval.forEach((rem) => rem()),
              forRemoval,
              count: 1
            })
            // TODO establish session
            addReq({ id, type, tickets, get: true })
          } else {
            const theRealm = rserv.realms.get(realmkey)
            theRealm.count++
            theRealm.add(remove)
          }
        }
        rserv.unRegister = (id, type, tickets) => {
          const realmkey = id + ':' + type
          const obj = rserv.realms.get(realmkey)
          // do we have to close something
          if (obj) {
            obj.count--
            if (obj.count === 0) {
              obj.remove()
              addReq({ type, tickets, stop: true })
              rserv.realms.delete(realmkey)
            }
          }
          if (rserv.realms.size === 0) {
            // TODO request for closing the connection
            addReq({ type, close: true })
            delete this.rservers[next]
          }
        }
        const value = Buffer.from(
          nextspki.split(':').map((el) => parseInt(el, 16))
        )
        const session = new WebTransport(next, {
          serverCertificateHashes: [{ algorithm: 'sha-256', value }]
        })
        try {
          // get token
          const response = await axios.get('/token', {
            ...this.axiosConfig(),
            params: { url: process.env.AVSROUTERURL }
          })
          if (!response.data || !response.data.token)
            throw new Error('no token retrieved')
          // end get token
          await session.ready
          const rs = session.incomingBidirectionalStreams
          const rsreader = rs.getReader()
          try {
            const { value } = await rsreader.read()
            if (value) {
              const awrt = value.writable.getWriter()
              const payload = BSONserialize({ token: response.data.token })
              await awrt.write(payload)
              await awrt.close()
              await value.readable.cancel(0)
            }
          } catch (error) {
            glog('error passing auth token reader', error)
          }
          rsreader.releaseLock()
        } catch (error) {
          glog('error passing auth token', error)
        }
        // rserv.session = session

        this.handleSession(session, rserv.requests)
          .finally(() => {
            // may be inform all clients listening
            delete this.rservers[next]
          })
          .catch((error) => {
            glog('Problem in handleSession remote:', error)
          })
        rserv.register(id, type, tickets, remove)
        return rserv
      } catch (error) {
        throw new Error('Prob getRemoteServer' + error)
      }
    }
  }

  unregisterStream(id, type, listener) {
    // console.trace()
    const realm = this.getRealmObj(id, type)
    if (realm.listeners.has(listener)) {
      realm.listeners.delete(listener)
      this.cleanUpRealm(id)
    } else {
      glog('listener to remove not present', listener)
      for (const listy of realm.listeners) {
        glog('Set debug', listy)
      }
      throw new Error('unRegisterStream failed ' + id + ' t ' + type)
    }
  }

  registerStream(id, type, listener) {
    const realm = this.getRealmObj(id, type)
    realm.listeners.add(listener)
  }

  unregisterClient(id, remote, router) {
    const realm = this.getRealmColl(id)
    if (remote) realm.numRclients--
    else if (router) realm.numRouter--
    else realm.numClients--
    this.cleanUpRealm(id)
    if (realm.numClients < 0 || realm.numRclients < 0 || realm.numRoute < 0) {
      console.trace()
      throw new Error('negative Client number')
    }
    if (remote && router) {
      console.trace()
      throw new Error('router and rclient')
    }
  }

  registerClient(id, remote, router) {
    const realm = this.getRealmColl(id)
    if (remote) realm.numRclients++
    else if (router) realm.numRouter++
    else realm.numClients++
    this.updateDispatch() // update the dispatcher
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

  createNonce() {
    const randombytes = new Uint8Array(16)
    crypto.getRandomValues(randombytes)
    return Array.from(new Uint16Array(randombytes.buffer))
      .map((el) => String(el).padStart(6, '0'))
      .join('')
  }

  cleanUpRealm(id) {
    const realm = this.realms[id]
    /* log(
      'CLEANUPREALM',
      realm.audio.listeners.size,
      realm.audio.localSources.size,
      realm.audio.remoteSources.size,
      realm.video.listeners.size,
      realm.video.localSources.size,
      realm.video.remoteSources.size,
      realm.screen.listeners.size,
      realm.screen.localSources.size,
      realm.screen.remoteSources.size,
      realm.audio.listeners.size === 0,
      realm.audio.localSources.size === 0,
      realm.audio.remoteSources.size === 0,
      realm.video.listeners.size === 0,
      realm.video.localSources.size === 0,
      realm.video.remoteSources.size === 0,
      realm.screen.listeners.size === 0,
      realm.screen.localSources.size === 0,
      realm.screen.remoteSources.size === 0,
      realm.numClients === 0,
      realm.numRclients === 0,
      realm.numRouter === 0
    ) */
    if (realm.numClients === 0 && realm.numRclients === 0) {
      realm.screen.rsession.forEach((el) => el.remove())
      realm.video.rsession.forEach((el) => el.remove())
      realm.audio.rsession.forEach((el) => el.remove()) // remove all remote sources from realm
    }
    if (
      realm.audio.listeners.size === 0 &&
      realm.audio.localSources.size === 0 &&
      realm.audio.remoteSources.size === 0 &&
      realm.video.listeners.size === 0 &&
      realm.video.localSources.size === 0 &&
      realm.video.remoteSources.size === 0 &&
      realm.screen.listeners.size === 0 &&
      realm.screen.localSources.size === 0 &&
      realm.screen.remoteSources.size === 0 &&
      realm.numClients === 0 &&
      realm.numRclients === 0
    ) {
      delete this.realms[id]
    }
  }

  async runServerLoop(server) {
    try {
      const sessionStream = await server.sessionStream('/avfails')
      const sessionReader = sessionStream.getReader()

      while (true) {
        const { done, value } = await sessionReader.read()
        if (done) {
          glog('Server exited')
          break
        }
        glog('new session on avsrouter ')
        this.handleSession(value)
      }
    } catch (error) {
      glog('problem in runServerLoop', error)
    }
  }

  async handleSession(session, requester) {
    // session log
    const peerAddress = session.peerAddress || 'noip'
    function slog() {
      const date = new Date().toLocaleString('en', { hour12: false })
      process.stdout.write('[' + date + ' ' + peerAddress + '] ')
      console.log.apply(console, arguments)
      return this
    }
    let sessionRunning = true
    const router = !!requester
    let counted = false
    let clientIsRouter = false
    const clientsRegistered = new Set()
    let primaryRealm

    const fetchers = {}
    const fetchersWrite = {}

    let taskTickets // only set if router

    let cleanupEntered = 0

    if (router) taskTickets = {}

    const cleanUpStuff = () => {
      cleanupEntered = 1
      if (counted) {
        if (!router && !clientIsRouter) this.sessionCount--
        else if (clientIsRouter) this.sessionCountRouterClients--
        else this.sessionCountRouters--
      }
      cleanupEntered = 2
      clientsRegistered.forEach((rclient) => {
        this.unregisterClient(rclient, clientIsRouter, router)
      })
      cleanupEntered = 3
      clientsRegistered.clear()
      // cleanup Fetchers
      cleanupEntered = 4
      for (const id in fetchersWrite) {
        for (const type in fetchersWrite[id]) {
          cleanupEntered = 5
          this.unregisterStream(id, type, fetchersWrite[id][type])
          delete fetchersWrite[id][type]
          delete fetchers[id][type]
        }
        delete fetchersWrite[id]
        delete fetchers[id]
      }
      cleanupEntered = 6
      // Cleanup realms

      if (primaryRealm) {
        this.primaryRealms[primaryRealm]--
        if (this.primaryRealms[primaryRealm] === 0)
          delete this.primaryRealms[primaryRealm]
      }
    }

    try {
      await session.ready
    } catch (error) {
      cleanUpStuff()
      slog('error session ready', error)
      return
    }
    slog('session is ready')
    session.closed
      .finally((reason) => {
        sessionRunning = false
        cleanUpStuff()
        slog(
          'session was closed:',
          reason,
          'clientisrouter:',
          clientIsRouter,
          'router:',
          !!router
        )
      })
      .catch((error) => {
        slog('Session error', error)
      })
    // const authorized = false // we are not yet authorized
    // get a bidi stream for authorizations, a Datagram will not be reliable
    const realmsReadable = []
    const realmsWritable = []
    if (!router) {
      try {
        let authstream = await session.createBidirectionalStream()
        const areader = await authstream.readable.getReader()
        // now we read all available data till the end for the token
        const timeout = setTimeout(() => {
          slog('authorization timeout')
          session.close({ reason: 'authorization timeout', closeCode: 408 })
        }, 10 * 1000) // one second max
        const readarr = []
        let asize = 0
        let csize = -1
        while (csize === -1 || asize < csize) {
          const readres = await areader.read()

          if (readres.value) {
            if (csize === -1) {
              csize = new DataView(readres.value.buffer).getInt32(0, true)
            }
            asize += readres.value.byteLength
            if (asize > 100000) throw new Error('authtoken too large') // prevent denial of service attack before auth
            readarr.push(readres.value)
          }
          if (readres.done) break
        }
        try {
          await areader.cancel()
        } catch (error) {
          slog('error areader cancel:', error)
        }
        try {
          await authstream.writable.close(200)
        } catch (error) {
          slog('error close writable', error)
        }
        authstream = null
        clearTimeout(timeout)
        if (asize < 5) throw new Error('Auth token too small abort')
        const cctoken = new Uint8Array(asize)
        readarr.reduce((previousValue, currentValue) => {
          cctoken.set(currentValue, previousValue)
          return previousValue + currentValue.byteLength
        }, 0)
        const jwttoken = BSONdeserialize(cctoken)
        const authtoken = await this.verifyToken(jwttoken.token)
        if (authtoken.accessRead)
          realmsReadable.push(
            ...authtoken.accessRead.map((el) => new RegExp(el))
          )
        if (authtoken.accessWrite)
          realmsWritable.push(
            ...authtoken.accessWrite.map((el) => new RegExp(el))
          )
        if (authtoken.router) {
          realmsReadable.push(/.+/)
          clientIsRouter = true
        } else if (authtoken.primaryRealm) {
          // client is no router, we can add the realm, to primaryRealms
          if (!this.primaryRealms[authtoken.primaryRealm]) {
            this.primaryRealms[authtoken.primaryRealm] = 0
          }
          this.primaryRealms[authtoken.primaryRealm]++
          primaryRealm = authtoken.primaryRealm
        }
      } catch (error) {
        slog('authorization stream failed', error)
        try {
          await new Promise((resolve) => setInterval(resolve, 1000)) // slow down potential attackers
          session.close({ reason: 'authorization failed', closeCode: 401 })
        } catch (error) {
          slog('auth failed session close failed', error)
        }
        return
      }
    }

    // counting of clients
    if (!router) {
      // only if not a client session
      if (!clientIsRouter) {
        if (this.sessionCount + 1 > parseInt(process.env.AVSMAXCLIENTS, 10)) {
          try {
            session.close({
              reason: 'maximum sessions reached',
              closeCode: 507
            })
          } catch (error) {
            slog('Error emergency session close', error)
          }
          this.updateDispatch() // update the dispatcher
          return
        }
        this.sessionCount++
      } else {
        this.sessionCountRouterClients++
      }
    } else {
      this.sessionCountRouters++
    }
    counted = true

    // ticket decode
    const ticketDecode = async ({ tickets, dir }) => {
      try {
        let routetics = tickets.map((el) => ({
          aeskey: el.aeskey ? el.aeskey.buffer : undefined,
          payload: el.payload ? el.payload.buffer : undefined,
          iv: el.iv ? el.iv.buffer : undefined
        }))
        const myticket = routetics.shift()
        const { aeskey, payload, iv } = myticket
        // first we need to decode the aeskey
        const aeskeydec = await crypto.subtle.importKey(
          'raw',
          await crypto.subtle.decrypt(
            {
              name: 'RSA-OAEP'
            },
            (
              await this.keypair
            ).privateKey,
            aeskey
          ),
          {
            name: 'AES-GCM',
            length: 256
          },
          false,
          ['decrypt']
        )
        // after getting the key we can decrypt, the actual data
        const {
          client,
          realm,
          next = undefined,
          nextspki = undefined,
          tempOut = undefined
        } = BSONdeserialize(
          await crypto.subtle.decrypt(
            {
              name: 'AES-GCM',
              iv
            },
            aeskeydec,
            payload
          )
        )

        // now we check, if it is actually allowed
        if (dir === 'outgoing') {
          // perspective of the router
          if (!realmsReadable.some((el) => el.test(client)) && !router) {
            throw new Error(
              'outgoing stream ' + client + ' not permitted or no router'
            )
          }
          if (!next && routetics.length > 0)
            throw new Error(
              'incoming stream ' + client + ' tickets without next'
            )
          if (next && routetics.length === 0)
            throw new Error(
              'incoming stream ' + client + ' next without tickets'
            )
        } else if (dir === 'incoming') {
          let tempPerm = false
          if (tempOut) {
            // okay we have a tempOut, let's check if it is valid
            const tempauthtoken = await this.verifyToken(tempOut)
            const temprealmsWritable = tempauthtoken.accessWrite.map(
              (el) => new RegExp(el)
            )
            if (temprealmsWritable.some((el) => el.test(client))) {
              tempPerm = true
            }
          }
          // perspective of the router
          if (
            !(realmsWritable.some((el) => el.test(client)) || tempPerm) &&
            !router
          ) {
            throw new Error(
              'incoming stream ' + client + ' not permitted or no router'
            )
          }
        }
        if (routetics.length === 0) routetics = undefined

        return { next, nextspki, tickets: routetics, client, realm } // todo add fetch logic
      } catch (error) {
        slog('problem decoding routing ticket', error)
        return undefined
      }
    }

    // process the type incoming stream
    const processIncomingStream = async (args) => {
      const streamreader = args.streamReader
      const stream = args.stream
      const id = args.id
      const type = args.type
      const parseHelper = args.parseHelper
      const quality = args.quality
      const incomStreamRunning = true

      slog('incoming stream with id:', id, 'and type:', type, 'qual:', quality)

      const paketcommitter = this.getPaketCommiter(
        id,
        type,
        quality,
        args.isRouter
      )
      const commitAndStoreMessage = this.getCommitAndStoreMessage(
        id,
        type,
        quality
      )
      const suspendQuality = this.getSuspendQuality(id, type, quality)

      let streamwriter

      let streamerror

      const writeStat = async (chunk) => {
        try {
          if (chunk.message) {
            // message are always passed, no delay there
            await streamwriter.write(chunk.message)
          }
        } catch (error) {
          if (!streamerror) {
            slog('first writeStat failed err:', error)
            slog('first writeStat failed writing', chunk.message)
            streamerror = 'writeStatfailed'
          }
        }
      }

      let statArray = []
      let insideSend = false

      const sendStatBson = async (chunk) => {
        statArray.push(chunk)
        if (statArray.length > 10) {
          if (!insideSend) {
            const sendArray = statArray
            statArray = []
            try {
              insideSend = true
              await this.sendBson(sendArray, writeStat)
            } catch (error) {
              throw new Error('sendBson err:', error)
            }
            insideSend = false
          } else {
            statArray.shift() // drop if blocked, no sense to pile up stat data
          }
        }
      }

      let curpaketsize
      const paketstat = (paket) => {
        if (clientIsRouter) slog('send Paket to router cIR', paket)
        if (streamerror) {
          slog('debug paket input', streamerror)
          console.trace()
        }
        if (paket.paketstart) {
          curpaketsize = paket.paket.byteLength
          sendStatBson({
            task: 'start',
            time: Date.now(),
            timestamp: new Decimal128(paket.timestamp.toString())
          }).catch((error) => {
            slog('problem stat:', error)
            streamerror = 'problem stat'
          })
        } else if (paket.paketend) {
          // currently obsolete
          curpaketsize += paket.paket.byteLength
          sendStatBson({
            task: 'end',
            size: curpaketsize
          }).catch((error) => {
            slog('problem stat:', error)
            streamerror = 'problem stat'
          })
          curpaketsize = 0
        } else {
          curpaketsize += paket.paket.byteLength
        }
      }

      try {
        streamwriter = await stream.writable.getWriter()

        // eslint-disable-next-line no-unmodified-loop-condition
        while (sessionRunning && incomStreamRunning) {
          // first the paket, as they are processed partially
          while (parseHelper.hasMessageOrPaket()) {
            const chunk = parseHelper.getMessageOrPaket()
            if (chunk.paket) {
              if (!args.isRouter) paketstat(chunk)
              // maybe also test if arraybuffer
              // if (args.isRouter) log('FIX QUAL READ')
              paketcommitter(chunk)
            } else {
              let store = false
              if (chunk.task && chunk.task === 'decoderconfig') {
                store = true
                slog('decoderconfig', JSON.stringify(chunk))
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
          if (streamerror) {
            const error = streamerror
            streamerror = undefined
            throw new Error('Streamerror:' + error)
          }
        }
      } catch (error) {
        slog(
          'error processIncomingStream with id:',
          id,
          'and type:',
          type,
          'qual:',
          quality,
          'and error:',
          error
        )
      }
      streamerror = 'stream is closed'
      try {
        this.removePaketCommiter(id, type, paketcommitter, args.isRouter)
        streamreader.releaseLock()
        if (streamwriter) streamwriter.releaseLock()
        /* await stream.writable.close()
        log('mark prob 4')
        await stream.readable.cancel()
        log('mark prob 6') */ // not needed
      } catch (error) {
        slog('error cleanup processIncomingStream', error)
      }
    }
    const processOutgoingStream = async (args) => {
      // log('processOutgoingStream', args)
      // TODO check AUTHENTIFICATION
      let streamreader = args.streamReader
      const stream = args.stream
      const streamrunning = args.running || (() => true)
      let outStreamRunning = true
      let curid = args.id
      let newid
      const type = args.type
      const parseHelper = args.parseHelper
      let curqual = -1
      let nextqual // tells us that we should change on the next keyframe
      let lastpaket = 0
      let streamwriterOut
      let writeChunk
      let fixQuality = this.getFixQuality(curid, type)
      let increaseQual = this.getIncreaseQual(curid, type)
      let decreaseQual = this.getDecreaseQual(curid, type)

      slog('outgoing stream with id:', curid, 'and type:', type)

      let dropmessage

      // eslint-disable-next-line no-unused-vars
      let paketsinwait = 0
      let qualchangeStor = []

      let outgoingbuffer = 0

      const outgoingpipe = new AsyncPaketPipe()

      if (args.fixedQuality) nextqual = args.fixedQuality

      try {
        streamwriterOut = await stream.writable.getWriter()
        const sIMsend = async (paket) => {
          try {
            outgoingpipe.addPaket(paket)
          } catch (error) {
            slog('error in sendInitialMessages 1', error)
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
          if (now - lastpaket > 1000 && !args.fixedQuality) {
            // recheck if quality is available
            const newqual = fixQuality(curqual)
            if (newqual !== curqual) {
              waitpaketstart = 1 // we need a key frame
              curqual = newqual
              slog('old quality stalled, set new quality', curqual)
              if (paketremain > 0) {
                // we finish the package with garbage, intentionally uninitalized
                const fakearray = new Uint8Array(new ArrayBuffer(paketremain))
                for (let i = 0; i < paketremain - 1; i++) fakearray[i] = 0
                fakearray[paketremain - 1] = 1 // ? why 1?
                outgoingpipe.addPaket(fakearray)
                inpaket = false
                paketremain = 0
              }
              // if changed may be emit an information for client ?
              try {
                await sendInitialMessages(curqual)
              } catch (error) {
                slog('Problem sendInitialMessages2', error)
                // no await! // init the decoder // no await, we do await!, it is the same queue
              }
            }
          }
          if (
            (typeof nextqual !== 'undefined' || typeof newid !== 'undefined') &&
            (quality === (nextqual || curqual) ||
              (nextqual || curqual) === -1) &&
            (pid === (newid || curid) || newid === 'sleep') &&
            ((chunk.paketstart && chunk.keyframe) || qualchangeStor.length > 0)
          ) {
            if (!inpaket) {
              // we can change
              if (typeof nextqual !== 'undefined') {
                curqual = nextqual
                nextqual = undefined
                if (!newid) {
                  try {
                    await sendInitialMessages(curqual)
                  } catch (error) {
                    slog('Problem sendInitialMessages2', error)
                    // no await! // init the decoder // no await, we do await!, it is the same queue
                  }
                }
              }
              if (typeof newid !== 'undefined') {
                slog('change id unregister stream peek', newid, curid)
                if (curid && curid !== 'sleep')
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
                curqual = fixQuality(curqual) // we may need to fix it...., if it is -1 or unavaliable
                slog('cur quality after change id:', curqual)
                newid = undefined
              }
              try {
                await sendInitialMessages(curqual)
              } catch (error) {
                slog('Problem sendInitialMessages3', error)
                // no await! // init the decoder // no await, we do await!, it is the same queue
              }
              for (const el of qualchangeStor) {
                inpaket = true
                if (el.paketend) inpaket = false
                // if (args.fixedQuality) log('FIX QUAL WRITE 3')
                outgoingpipe.addPaket(el)
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
          if (!inpaket && outgoingbuffer > 1000000) {
            if (!dropmessage) {
              dropmessage = true
              waitpaketstart = 1
            }
            return
          }
          try {
            if (chunk.paket) {
              lastpaket = now
              if (waitpaketstart) {
                if (chunk.paketstart && chunk.keyframe)
                  waitpaketstart = 0 // we need to start with complete pakets and a keyframe!
                else return
              }
              dropmessage = false
              inpaket = true
              if (chunk.paketend) inpaket = false
              if (chunk.paketremain) paketremain = chunk.paketremain
              else paketremain = 0
              outgoingbuffer += chunk.paket.byteLength
              // if (args.fixedQuality) log('FIX QUAL WRITE 1', quality)
              // log('Pakets in wait', type, paketsinwait, outgoingbuffer)
              paketsinwait++
              outgoingpipe.addPaket(chunk)

              // if (args.fixedQuality) log('FIX QUAL WRITE 1a', quality)
            } else if (chunk.message) {
              // message are always passed, no delay there
              // if (args.fixedQuality) log('FIX QUAL WRITE 2')
              outgoingpipe.addPaket(chunk)
            }
          } catch (error) {
            if (writefailedres) {
              slog('writefailed', error)
              writefailedres('writefailed')
              writefailedres = undefined
            }
            outStreamRunning = false
          }
        }
        const writing = async () => {
          let dropcurrentpaketwrite = false
          let dropuntilkeyframe = 10 // support max 10 layers, should be save
          let curdroptime = type === 'audio' ? 100 : 40 // video should be dropped faster
          let temporalLayerId = 0
          let candecreaseQual = true
          try {
            // eslint-disable-next-line no-unmodified-loop-condition
            while (sessionRunning && outStreamRunning && streamrunning()) {
              const chunk = await outgoingpipe.getPaket()
              const now = Date.now()

              if (chunk.paket) {
                if (chunk.temporalLayerId !== undefined)
                  temporalLayerId = chunk.temporalLayerId
                else {
                  slog(
                    'PANIC PANIC PANIC no TEMPORAL LAYER',
                    type,
                    chunk.temporalLayerId
                  )
                }
                if (
                  dropuntilkeyframe !== 10 &&
                  !dropcurrentpaketwrite &&
                  chunk.paketstart &&
                  chunk.keyframe
                ) {
                  dropuntilkeyframe = 10 // includes all ten layers
                }
                // ok now we determine, if we should drop the paket since it is sitting here for a while
                // we should use hysteresis, once we drop we drop completely
                if (chunk.paketstart && chunk.incomtime) {
                  if (now - chunk.incomtime > curdroptime) {
                    slog(
                      'outgoing writer DROP, time delay',
                      type,
                      temporalLayerId,
                      now - chunk.incomtime,
                      curdroptime,
                      router
                    )
                    // if we are jammed for 1 second, we should decrease quality
                    if (now - chunk.incomtime > 1000 && candecreaseQual) {
                      const newqual = decreaseQual(curqual)
                      slog('decqual inside DROP', newqual, nextqual)
                      if (newqual !== curqual) {
                        nextqual = newqual
                        qualchangeStor = [] // reset any already ongoing change
                      }
                    }
                    dropcurrentpaketwrite = true
                    curdroptime = Math.max(20, curdroptime - 30)
                    if (
                      chunk.temporalLayerId &&
                      dropuntilkeyframe > chunk.temporalLayerId
                    )
                      dropuntilkeyframe = chunk.temporalLayerId
                    if (chunk.keyframe) dropuntilkeyframe = 0 // if we have a keyframe and drop, we must drop until the next one
                  } else {
                    curdroptime = type === 'audio' ? 100 : 40 // reset
                    candecreaseQual = true
                  }
                }
                if (
                  !dropcurrentpaketwrite &&
                  dropuntilkeyframe > temporalLayerId
                ) {
                  await streamwriterOut.write(chunk.paket)
                }
                paketsinwait--
                outgoingbuffer -= chunk.paket.byteLength

                if (chunk.paketend) {
                  dropcurrentpaketwrite = false
                }
              } else if (chunk.message) {
                // log('messagesend', chunk.message.byteLength)
                await streamwriterOut.write(chunk.message)
              }
            }
            await streamwriterOut.close()
          } catch (error) {
            if (writefailedres) {
              slog('writefailed writing', error)
              writefailedres('writefailed')
              writefailedres = undefined
            }
            outStreamRunning = false
          }
          streamwriterOut.releaseLock()
          streamwriterOut = undefined
        }
        this.registerStream(curid, type, writeChunk)
        const reading = async () => {
          try {
            // eslint-disable-next-line no-unmodified-loop-condition
            while (sessionRunning && outStreamRunning && streamrunning()) {
              while (parseHelper.hasMessageOrPaket()) {
                // only messages for controlling
                const message = parseHelper.getMessageOrPaket() // process them, e.g. change quality of stream
                if (message.task === 'incQual') {
                  const newqual = increaseQual(curqual)
                  if (newqual !== curqual) {
                    nextqual = newqual
                    qualchangeStor = [] // reset any already ongoing change
                    slog('incqual', newqual, nextqual)
                  }
                } else if (message.task === 'decQual') {
                  const newqual = decreaseQual(curqual)
                  if (newqual !== curqual) {
                    nextqual = newqual
                    qualchangeStor = [] // reset any already ongoing change
                  }
                  slog('decqual', newqual, nextqual)
                } else if (message.task === 'chgId') {
                  let messid
                  let dectics
                  if (message.tickets) {
                    dectics = await ticketDecode({
                      tickets: message.tickets,
                      dir: 'outgoing' // routers perspective
                    })
                    if (dectics) {
                      messid = dectics.client
                      slog('client info for change', JSON.stringify(dectics))
                    }
                  }
                  slog('incoming change', curid, messid)

                  if (curid !== messid) {
                    // we do not need the next line, writeChunk takes care about unregistering
                    // this.unregisterStream(curid, type, writeChunk)
                    // check if stream is available and not acquire it from remote
                    if (messid) {
                      slog('change to', messid)
                      // TODO check AUTHENTIFICATION, done in the ticket
                      if (dectics) {
                        if (!clientsRegistered.has(dectics.client)) {
                          this.registerClient(
                            dectics.client,
                            clientIsRouter,
                            router
                          )
                          clientsRegistered.add(dectics.client)
                        }
                      }
                      if (dectics && dectics.next) {
                        await this.acquireStreams({
                          next: dectics.next,
                          nextspki: dectics.nextspki,
                          tickets: dectics.tickets,
                          id: messid,
                          type
                        })
                      }
                      newid = messid
                      const checkqual = fixQuality(curqual)
                      if (checkqual !== curqual) nextqual = checkqual
                      slog(
                        'registerStream in change',
                        newid,
                        type,
                        nextqual,
                        curqual,
                        checkqual
                      )

                      this.registerStream(newid, type, writeChunk)
                    } else {
                      newid = 'sleep'
                    }
                    qualchangeStor = [] // reset any already ongoing change
                  }
                }
              }

              const readres = await streamreader.read()

              if (readres.value) parseHelper.addPaket(readres.value)
              if (readres.done) break
            }
            // close the stream reader
            await streamreader.cancel()
          } catch (error) {
            slog('reading failed', error)
          }
          streamreader.releaseLock()
          streamreader = undefined
        }
        await Promise.race([reading(), writefailed, writing()])
      } catch (error) {
        slog('error processOutgoingStream', error)
      }
      outgoingpipe.flush()
      try {
        if (curid !== 'sleep') this.unregisterStream(curid, type, writeChunk)
        if (newid && newid !== 'sleep')
          this.unregisterStream(newid, type, writeChunk)
      } catch (error) {
        slog('error cleanup processIncomingStream', error)
      }
      outStreamRunning = false
    }

    // our stream fetcher
    const processStreamFetcher = ({ id, type, tickets, nonce }) => {
      // ok we install listeners to automatically fetch all qualities

      const streams = {} // indexed by quality
      if (!fetchers[id]) fetchers[id] = {}
      if (fetchers[id][type]) {
        slog('double fetcher', id, type, fetchers[id][type])
        slog('fetcher state', fetchers)
        slog('fetchers write', fetchersWrite[id])
        console.trace()
        throw new Error('Should not happen double fetcher')
      }
      slog('PROCESSTREAMFETCHER mark1')
      fetchers[id][type] = streams

      const writeTest = async (chunk, pid, quality) => {
        if (!streams[quality]) {
          try {
            streams[quality] = session.createBidirectionalStream() // means we have no stream
            streams[quality] = await streams[quality]
            const stream = streams[quality]
            // ok now configure the stream
            const writer = stream.writable.getWriter()
            const writFunc = async (chunk) => {
              try {
                await writer.write(chunk.message)
              } catch (error) {
                throw new Error('stream fetch writer failed:', error)
              }
            }

            await this.sendBson(
              {
                command: 'configure',
                dir: 'incoming', // routers perspective
                quality,
                type,
                nonce
              },
              writFunc
            )
            writer.releaseLock()
            processOutgoingStream({
              stream,
              streamReader: await streams[quality].readable.getReader(),
              parseHelper: new ParseHelper(),
              id,
              type,
              fixedQuality: quality,
              running: () => !!streams[quality]
            })
              .finally(() => {
                if (streams[quality] === stream) delete streams[quality]
                slog('PROCESSSTREAMFETCHER finally after', streams)
              })
              .catch((error) =>
                slog('fetch processOutgoing Stream problem', error)
              )
            // ok now we can install, sth that passes on the data?
          } catch (error) {
            slog('fetch streams error:', cleanupEntered, error)
            // throw new Error('failure fetch stream:' + error) // do not break
          }
        }
      }

      this.registerStream(id, type, writeTest)
      if (!fetchersWrite[id]) fetchersWrite[id] = {}
      if (fetchersWrite[id][type]) {
        slog('double fetcher write', id, type, fetchers[id][type])
        slog('fetcher state', fetchers)
        throw new Error('Should not happen double fetcher write')
      }
      fetchersWrite[id][type] = writeTest
    }

    const processRemoveStreamFetcher = ({ id, type }) => {
      if (fetchers[id] && fetchers[id][type]) {
        const closestreams = async () => {
          try {
            const allstreams = fetchers[id][type]
            delete fetchers[id][type]
            if (Object.keys(fetchers[id]).length === 0) delete fetchers[id]
            for (const quality in allstreams) {
              delete allstreams[quality] // this triggers, that it is not running
            }
            // const cancels = streams.map((stream) => stream.readable.cancel())
            // const closes = streams.map((stream) => stream.writable.close())
            // await Promise.all([/* ...cancels, * ...closes])
          } catch (error) {
            slog('processRemoveFetcher: ', error)
          }
        }
        closestreams().catch((error) => {
          slog('Problem ProcessRemoveStreamFetcher streams:', error)
        })
      }
      if (fetchersWrite[id] && fetchersWrite[id][type]) {
        const writeTest = fetchersWrite[id][type]
        delete fetchersWrite[id][type]
        if (Object.keys(fetchersWrite[id]).length === 0)
          delete fetchersWrite[id]
        try {
          this.unregisterStream(id, type, writeTest)
        } catch (error) {
          throw new Error('Problem processRemoveStreamFetcher: ', error)
        }
      }
    }

    // our stream processor
    const processStream = async (stream) => {
      let pspos = -1
      try {
        pspos = 1
        const streamReader = await stream.readable.getReader()
        const parseHelper = new ParseHelper()
        pspos = 2
        let garbage = 0
        // eslint-disable-next-line no-unmodified-loop-condition
        while (sessionRunning) {
          pspos = 3
          const paket = await streamReader.read()
          pspos = 4
          if (paket.value) parseHelper.addPaket(paket.value)
          if (paket.done) break
          pspos = 5
          if (parseHelper.hasMessageOrPaket()) {
            pspos = 6
            garbage = 0
            const message = parseHelper.getMessageOrPaket()
            pspos = 7
            if (!clientIsRouter) {
              pspos = 8
              if (
                message.command === 'configure' &&
                (message.dir === 'incoming' || message.dir === 'outgoing') &&
                (message.tickets || message.dir === 'incoming') &&
                (message.dir === 'outgoing' || message.quality) &&
                (message.type === 'video' ||
                  message.type === 'audio' ||
                  message.type === 'screen') &&
                (!router || (message.nonce && message.dir === 'incoming'))
              ) {
                let dectics
                pspos = 9

                if (!router) {
                  dectics = await ticketDecode({
                    tickets: message.tickets,
                    dir: message.dir
                  })
                } else {
                  dectics = taskTickets[message.nonce]
                  // delete taskTickets[message.nonce] // my bad, nonce is actually used multiple times for every quality, type
                }
                pspos = 10
                pspos = 11
                if (!dectics) {
                  streamReader.releaseLock()
                  pspos = 12
                  await stream.readable.cancel(403)
                  pspos = 13
                  break
                }

                // later may be routing code
                if (message.dir === 'incoming') {
                  pspos = 14
                  await processIncomingStream({
                    stream,
                    streamReader,
                    parseHelper,
                    id: dectics.client,
                    type: message.type,
                    quality: message.quality,
                    isRouter: !!router
                  })
                  pspos = 15
                  break
                } else if (message.dir === 'outgoing') {
                  pspos = 16
                  if (!clientsRegistered.has(dectics.client)) {
                    this.registerClient(dectics.client, clientIsRouter, router)
                    clientsRegistered.add(dectics.client)
                  }
                  pspos = 17
                  if (dectics.next) {
                    await this.acquireStreams({
                      next: dectics.next,
                      nextspki: dectics.nextspki,
                      tickets: dectics.tickets,
                      id: dectics.client,
                      type: message.type
                    })
                  }
                  pspos = 18
                  pspos = 19
                  await processOutgoingStream({
                    stream,
                    streamReader,
                    parseHelper,
                    id: dectics.client,
                    type: message.type
                  })
                  break
                }
              } else if (message.command !== 'nop') {
                pspos = 20
                streamReader.releaseLock()
                await stream.readable.cancel(403)
                break
              } else slog('NOP')
            } else {
              pspos = 21
              // case clientIsRouter
              if (
                message.command === 'fetchStreams' &&
                /* message.id && */
                (message.type === 'video' ||
                  message.type === 'audio' ||
                  message.type === 'screen')
              ) {
                pspos = 22
                const dectics = await ticketDecode({
                  tickets: message.tickets,
                  dir: 'outgoing'
                })
                pspos = 23
                if (!dectics) {
                  streamReader.releaseLock()
                  await stream.readable.cancel(403)
                  break
                }
                pspos = 24
                if (!clientsRegistered.has(dectics.client)) {
                  this.registerClient(dectics.client, clientIsRouter, router)
                  clientsRegistered.add(dectics.client)
                }
                pspos = 25
                if (dectics.next) {
                  await this.acquireStreams({
                    next: dectics.next,
                    nextspki: dectics.nextspki,
                    tickets: dectics.tickets,
                    id: dectics.client,
                    type: message.type
                  })
                }
                pspos = 26
                // now we install a stream fetcher
                pspos = 27
                processStreamFetcher({
                  id: dectics.client,
                  type: message.type,
                  nonce: message.nonce,
                  tickets: dectics.tickets
                })
              } else if (
                message.command === 'stopFetchStreams' &&
                message.tickets &&
                (message.type === 'video' ||
                  message.type === 'audio' ||
                  message.type === 'screen')
              ) {
                pspos = 28
                const dectics = await ticketDecode({
                  tickets: message.tickets,
                  dir: 'outgoing'
                })
                pspos = 29
                if (!dectics) {
                  streamReader.releaseLock()
                  await stream.readable.cancel(403)
                  break
                }
                pspos = 30
                processRemoveStreamFetcher({
                  id: dectics.client,
                  type: message.type
                })
                pspos = 31
              } else {
                slog('unknown command close', message)
                streamReader.releaseLock()
                await stream.readable.cancel(400)
                break
              }
            }
          } else {
            pspos = 32
            garbage++
            if (garbage > 10) {
              slog('reject stream with GARBAGE')
              stream.close(400)
              // or should we disconnect the whole session?
              break
            }
          }
          slog('paket loop end')
          pspos = 33
        }
        slog('processStream exited')
      } catch (error) {
        slog('error in processStream', pspos, error)
        sessionRunning = false
      }
      // do not add anything outside the try catch
    }

    const streamProcess = async () => {
      let bidicount = 0
      // now, we process every incoming bidistream and see what it wants
      try {
        const bidiReader = session.incomingBidirectionalStreams.getReader()
        // eslint-disable-next-line no-unmodified-loop-condition
        while (sessionRunning) {
          const bidistr = await bidiReader.read()
          if (bidistr.done) {
            slog('bidiReader terminated', clientIsRouter, router)
            break
          }
          if (bidistr.value) {
            bidicount++
            slog('incoming bidirectional stream', bidicount)
            processStream(bidistr.value).catch((error) => {
              slog('sP problem processStream', error)
            })
          }
        }
      } catch (error) {
        slog('bidirectional reader exited with', error)
      }
    }
    const requestProcess = async () => {
      try {
        const bidistrCtrl = await session.createBidirectionalStream()
        const bidiWriter = await bidistrCtrl.writable.getWriter()
        const writFunc = async (chunk) => {
          try {
            await bidiWriter.write(chunk.message)
          } catch (error) {
            slog('Bidiwriter failed:', error)
            throw new Error('Bidiwriter failed:', error)
          }
        }
        // TODO authentification
        while (true) {
          const request = await requester[0]
          requester.shift()
          if (request.close) {
            try {
              await session.close({
                reason: 'close requested',
                closeCode: 501
              })
            } catch (error) {
              slog('problem during close request:', error)
              break
            }
            break
          }
          if (request.get) {
            const { tickets, type, id } = request
            const nonce = this.createNonce()

            taskTickets[nonce] = { client: id } // that is our ticket, already decoded
            // ok we should acquire all, streams connected with id and type..., as long as the request works
            try {
              await this.sendBson(
                {
                  command: 'fetchStreams',
                  tickets, // no id!
                  type,
                  nonce
                },
                writFunc
              )
            } catch (error) {
              slog('problem during get request:', error)
              break
            }
          }
          if (request.stop) {
            // eslint-disable-next-line no-unused-vars
            const { tickets, type, id /* only for debugging */ } = request
            // ok we should acquire all, streams connected with id and type..., as long as the request works
            try {
              await this.sendBson(
                {
                  command: 'stopFetchStreams',
                  tickets,
                  type
                },
                writFunc
              )
            } catch (error) {
              slog('problem during stop request:', error)
              break
            }
          }
          // do stuff
        }
        try {
          bidiWriter.releaseLock()
        } catch (error) {
          slog('problem releasing Writer', error)
        }
        try {
          // bidistrCtrl.writable.close(501)
          await bidistrCtrl.readable.cancel(501)
        } catch (error) {
          slog('problem closing stream', error)
        }
      } catch (error) {
        slog('Problem during requests:', error)
      }
    }
    try {
      if (router) await Promise.all([streamProcess(), requestProcess()])
      else await Promise.all([streamProcess()])
    } catch (error) {
      slog('problem in streamProcess/requestProcess', error)
    }
  }
}
