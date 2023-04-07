import {
  serialize as BSONserialize,
  deserialize as BSONdeserialize,
  Decimal128
} from 'bson'
import jwt from 'jsonwebtoken'
import axios from 'axios'
import { webcrypto as crypto } from 'crypto'
import { WebTransport } from '@fails-components/webtransport'

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
      return Promise.resolve(this.pakets.shift())
    } else {
      return new Promise((resolve, reject) => {
        this.waitpromres.push(resolve)
        this.waitpromrej.push(reject)
      })
    }
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
                timestamp,
                incomtime: Date.now()
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
    this.rservers = {}
    this.spki = args.spki
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
      console.log('problem generating privkey pair', error)
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
        console.log('debug router realms', this.realms)
        for (const client in this.realms)
          console.log('clients', this.realms[client].video.listeners)
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
        console.log('realmInfo', realmInfo)
        const numRealms = new Set(realmInfo.map((el) => el.realm)).size

        const primRealms = Object.entries(this.primaryRealms)
          .filter((el) => el[1] > 0)
          .map((el) => el[0])

        console.log('debug router info', {
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
        })

        await axios.put(
          '/router',
          {
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
          },
          { ...this.axiosConfig() }
        )
      } catch (error) {
        console.log('problem updating dispatch info: ', error)
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
      console.log('Error fetchKey', err)
      throw new Error('no key obtained')
    }
  }

  async verifyToken(token) {
    // console.log('peek verify token', token)
    const decoded = jwt.decode(token)
    // console.log('decoded jwt', decoded)
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
    console.log('DEBUG getPaketCommiter', id, type, quality, isRouter)
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
      console.log('problem send bson', tosend)
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
    console.log('acquireStreams before check', id, realm)
    if (realm.localSources.size > 0 || realm.remoteSources.size > 0) return
    if (realm.rsession.size > 0) return
    console.log('acquireStreams after check')
    // we do not have a local source or a remote session attached, we have to attach a new session
    let removed = false
    const myrsession = {
      remove: () => {
        console.log('RSESSION EMPTY REMOVE CALLED')
        removed = true
      }
    }
    realm.rsession.add(myrsession)
    try {
      console.log('AQS 1')
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
      console.log('AQS 2')
      if (removed) {
        serv.unRegister(id, type, tickets)
      }
      console.log('AQS 3')
      myrsession.remove = () => {
        console.log('RSESSION REMOVE CALLED')
        console.trace()
        serv.unRegister(id, type, tickets)
        realm.rsession.delete(myrsession)
      }
      console.log('AQS 4')
    } catch (error) {
      throw new Error('Acquire Streams prob:' + error)
    }
  }

  async getRemoteServer({ next, nextspki, remove, id, type, tickets }) {
    console.log('getRemoteServer', this.rservers, next)
    if (this.rservers[next]) {
      console.log('gRS 1')
      const serv = this.rservers[next]
      console.log('gRS 2')
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
        console.log('gRS 10')
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
        console.log('gRS 11')
        rserv.register = (id, type, tickets, remove) => {
          const realmkey = id + ':' + type
          console.log('REGISTER, all realms', realmkey)
          console.log('REGISTER, all realms2', this.realms)
          console.log('REGISTER, rserv realms', rserv.realms)
          if (!rserv.realms.has(realmkey)) {
            const forRemoval = new Set()
            forRemoval.add(remove)
            // already
            rserv.realms.set(realmkey, {
              remove: () => forRemoval.forEach((rem) => rem()),
              forRemoval,
              count: 1
            })
            console.log('REGISTER, all realms after set', realmkey)
            console.log('REGISTER, rserv realms after set', rserv.realms)
            // TODO establish session
            console.log('REGISTER CALLED ON remote', id, type, remove)
            addReq({ id, type, tickets, get: true })
          } else {
            const theRealm = rserv.realms.get(realmkey)
            theRealm.count++
            theRealm.add(remove)
          }
        }
        console.log('gRS 12')
        rserv.unRegister = (id, type, tickets) => {
          console.log('UNREGISTER CALLED ON remote', id, type)
          const realmkey = id + ':' + type
          const obj = rserv.realms.get(realmkey)
          // do we have to close something
          if (obj) {
            obj.count--
            if (obj.count === 0) {
              console.log('DELETE REQUEST')
              obj.remove()
              addReq({ type, tickets, stop: true })
              rserv.realms.delete(realmkey)
            }
          }
          if (rserv.realms.size === 0) {
            console.log('CLOSE REQUEST')
            // TODO request for closing the connection
            addReq({ type, close: true })
            delete this.rservers[next]
          }
        }
        console.log('gRS 13')

        console.log('gRS 3a', next)
        console.log('gRS 3b', nextspki)
        const value = Buffer.from(
          nextspki.split(':').map((el) => parseInt(el, 16))
        )
        const session = new WebTransport(next, {
          serverCertificateHashes: [{ algorithm: 'sha-256', value }]
        })
        console.log('gRS 4')
        try {
          console.log('gRS 5')
          // get token
          const response = await axios.get('/token', {
            ...this.axiosConfig(),
            params: { url: process.env.AVSROUTERURL }
          })
          console.log('gRS 6')
          if (!response.data || !response.data.token)
            throw new Error('no token retrieved')
          // end get token
          console.log('gRS 7')
          await session.ready
          console.log('gRS 7b')
          const rs = session.incomingBidirectionalStreams
          const rsreader = rs.getReader()
          try {
            const { value } = await rsreader.read()
            if (value) {
              const awrt = value.writable.getWriter()
              const payload = BSONserialize({ token: response.data.token })
              await awrt.write(payload)
              await awrt.close()
              value.readable.cancel(0)
            }
          } catch (error) {
            console.log('error passing auth token reader', error)
          }
          console.log('gRS 8')
          rsreader.releaseLock()
        } catch (error) {
          console.log('error passing auth token', error)
        }
        console.log('gRS 9')
        // rserv.session = session

        this.handleSession(session, rserv.requests)
          .finally(() => {
            // may be inform all clients listening
            delete this.rservers[next]
          })
          .catch((error) => {
            console.log('Problem in handleSession remote:', error)
          })
        rserv.register(id, type, tickets, remove)
        return rserv
      } catch (error) {
        throw new Error('Prob getRemoteServer' + error)
      }
    }
  }

  unregisterStream(id, type, listener) {
    console.log('DEBUG unregister Stream', id, type, listener)
    console.trace()
    const realm = this.getRealmObj(id, type)
    if (realm.listeners.has(listener)) {
      realm.listeners.delete(listener)
      this.cleanUpRealm(id)
    } else {
      console.log('listener to remove not present', listener)
      for (const listy of realm.listeners) {
        console.log('Set debug', listy)
      }
      throw new Error('unRegisterStream failed ' + id + ' t ' + type)
    }
  }

  registerStream(id, type, listener) {
    console.log('DEBUG register Stream', id, type, listener)
    const realm = this.getRealmObj(id, type)
    realm.listeners.add(listener)
  }

  unregisterClient(id, remote, router) {
    console.log('DEBUG unregister Client', id, remote)
    const realm = this.getRealmColl(id)
    if (remote) realm.numRclients--
    else if (router) realm.numRouter--
    else realm.numClients--
    this.cleanUpRealm(id)
    if (realm.numClients < 0 || realm.numRclients < 0 || realm.numRoute < 0) {
      console.log('realm debug neg', id, remote, router, realm)
      console.trace()
      throw new Error('negative Client number')
    }
    if (remote && router) {
      console.log('realm debug remote router', id, remote, router)
      console.trace()
      throw new Error('router and rclient')
    }
  }

  registerClient(id, remote, router) {
    console.log('DEBUG register Client', id, remote)
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
    console.log('CLEANUPREALM', id)
    const realm = this.realms[id]
    console.log(
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
    )
    if (realm.numClients === 0 && realm.numRclients === 0) {
      console.log(
        'DELETE CALL REMOVE RESESSION',
        realm.screen.rsession.size,
        realm.video.rsession.size,
        realm.audio.rsession.size,
        id
      )
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
      console.log('realm with id delete', id)
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
          console.log('Server is exited')
          break
        }
        console.log('new session on avsrouter ')
        this.handleSession(value)
      }
    } catch (error) {
      console.log('problem in runServerLoop', error)
    }
  }

  async handleSession(session, requester) {
    let running = true
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
      console.log('CLEANUP 1')
      cleanupEntered = 1
      if (counted) {
        if (!router && !clientIsRouter) this.sessionCount--
        else if (clientIsRouter) this.sessionCountRouterClients--
        else this.sessionCountRouters--
      }
      console.log('CLEANUP 2')
      cleanupEntered = 2
      clientsRegistered.forEach((rclient) => {
        console.log('cleanup unregister', rclient)
        this.unregisterClient(rclient, clientIsRouter, router)
      })
      console.log('CLEANUP 3')
      cleanupEntered = 3
      clientsRegistered.clear()
      // cleanup Fetchers
      cleanupEntered = 4
      console.log('CLEANUP FETCHERS', fetchersWrite)
      for (const id in fetchersWrite) {
        for (const type in fetchersWrite[id]) {
          cleanupEntered = 5
          console.log('cleanupfetcher', id, type)
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
      console.log('error session ready')
      return
    }
    console.log('session is ready')
    session.closed
      .finally((reason) => {
        cleanUpStuff()
        console.log(
          'session was closed:',
          reason,
          'clientisrouter:',
          clientIsRouter,
          'router:',
          !!router
        )
      })
      .catch((error) => {
        console.log('Session error', error)
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
          console.log('authorization timeout')
          session.close({ reason: 'authorization timeout', closeCode: 408 })
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
              // console.log('csize', csize)
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
          console.log('error areader cancel:', error)
        }
        try {
          await authstream.writable.close(200)
        } catch (error) {
          console.log('error close writable', error)
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
        // console.log('peak jwttoken', jwttoken)
        const authtoken = await this.verifyToken(jwttoken.token)
        // console.log('peak authtoken', authtoken)
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
        console.log('authorization stream failed', error)
        try {
          await new Promise((resolve) => setInterval(resolve, 1000)) // slow down potential attackers
          session.close({ reason: 'authorization failed', closeCode: 401 })
        } catch (error) {
          console.log('auth failed session close failed', error)
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
            console.log('Error emergency session close', error)
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
          nextspki = undefined
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
        // console.log('realms peak', realmsReadable, realmsWritable)
        if (dir === 'outgoing') {
          // perspective of the router
          if (!realmsReadable.some((el) => el.test(client)) && !router) {
            console.log('router mode', router)
            console.log('realms readable', realmsReadable)
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
          // perspective of the router
          if (!realmsWritable.some((el) => el.test(client)) && !router) {
            console.log('router mode', router)
            throw new Error(
              'incoming stream ' + client + ' not permitted or no router'
            )
          }
        }
        if (routetics.length === 0) routetics = undefined

        return { next, nextspki, tickets: routetics, client, realm } // todo add fetch logic
      } catch (error) {
        console.log('problem decoding routing ticket', error)
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
            console.log('first writeStat failed err:', error)
            console.log('first writeStat failed writing', chunk.message)
            streamerror = 'writeStatfailed'
          }
        }
      }

      const sendStatBson = async (chunk) => {
        try {
          await this.sendBson(chunk, writeStat)
        } catch (error) {
          throw new Error('sendBson err:', error)
        }
      }

      let curpaketsize
      const paketstat = (paket) => {
        if (clientIsRouter) console.log('send Paket to router cIR', paket)
        if (streamerror) {
          console.log('debug paket input', streamerror)
          console.trace()
        }
        if (paket.paketstart) {
          curpaketsize = paket.paket.byteLength
          sendStatBson({
            task: 'start',
            time: Date.now(),
            timestamp: new Decimal128(paket.timestamp.toString())
          }).catch((error) => {
            console.log('problem stat:', error)
            streamerror = 'problem stat'
          })
        } else if (paket.paketend) {
          curpaketsize += paket.paket.byteLength
          sendStatBson({
            task: 'end',
            time: Date.now(),
            size: curpaketsize
          }).catch((error) => {
            console.log('problem stat:', error)
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
        while (running) {
          // first the paket, as they are processed partially
          while (parseHelper.hasMessageOrPaket()) {
            const chunk = parseHelper.getMessageOrPaket()
            if (chunk.paket) {
              paketstat(chunk)
              // maybe also test if arraybuffer
              // if (args.isRouter) console.log('FIX QUAL READ')
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
          if (streamerror) {
            const error = streamerror
            streamerror = undefined
            throw new Error('Streamerror:' + error)
          }
        }
      } catch (error) {
        console.log('error processIncomingStream', error)
      }
      streamerror = 'stream is closed'
      try {
        this.removePaketCommiter(id, type, paketcommitter, args.isRouter)
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
      // console.log('processOutgoingStream', args)
      // TODO check AUTHENTIFICATION
      let streamreader = args.streamReader
      const stream = args.stream
      const streamrunning = args.running || (() => true)
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

      let pakettime
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
            console.log('error in sendInitialMessages 1', error)
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
            // recheck quality
            const newqual = fixQuality(curqual)
            if (newqual !== curqual) {
              waitpaketstart = 1 // we need a key frame
              curqual = newqual
              console.log('new quality', curqual)
              if (paketremain > 0) {
                // we finish the package with garbage, intentionelly uninitalized
                const fakearray = new Uint8Array(new ArrayBuffer(paketremain))
                for (let i = 0; i < paketremain - 1; i++) fakearray[i] = 0
                fakearray[paketremain - 1] = 1
                outgoingpipe.addPaket(fakearray)
                inpaket = false
                paketremain = 0
              }
              // if changed may be emit an information for client ? TODO
              sendInitialMessages(curqual).catch((error) => {
                console.log('Problem sendInitialMessages', error)
              }) // no await!
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
                console.log('change id unregister stream peek', newid, curid)
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
                newid = undefined
              }
              sendInitialMessages(curqual).catch((error) => {
                console.log('Problem sendInitialMessages2', error)
              }) // no await! // init the decoder // no await
              for (const el of qualchangeStor) {
                inpaket = true
                if (el.paketend) inpaket = false
                // if (args.fixedQuality) console.log('FIX QUAL WRITE 3')
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
              console.log(
                'outgoing buffer DROP size',
                type,
                outgoingbuffer,
                chunk?.paket?.byteLength
              )
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
              // if (args.fixedQuality) console.log('FIX QUAL WRITE 1', quality)
              // console.log('Pakets in wait', type, paketsinwait, outgoingbuffer)
              paketsinwait++
              outgoingpipe.addPaket(chunk)

              // if (args.fixedQuality) console.log('FIX QUAL WRITE 1a', quality)
            } else if (chunk.message) {
              // message are always passed, no delay there
              // if (args.fixedQuality) console.log('FIX QUAL WRITE 2')
              outgoingpipe.addPaket(chunk)
            }
          } catch (error) {
            if (writefailedres) {
              console.log('DEBUG writefailed', error)
              writefailedres('writefailed')
              writefailedres = undefined
            }
            running = false
          }
        }
        const writing = async () => {
          let dropcurrentpaketwrite = false
          let dropuntilkeyframe = false
          let curdroptime = 100
          // eslint-disable-next-line no-unmodified-loop-condition
          while (running && streamrunning()) {
            try {
              const chunk = await outgoingpipe.getPaket()
              const now = Date.now()

              if (chunk.paket) {
                if (
                  dropuntilkeyframe &&
                  !dropcurrentpaketwrite &&
                  chunk.paketstart &&
                  chunk.keyframe
                ) {
                  dropuntilkeyframe = false
                }
                // ok now we determine, if we should drop the paket since it is sitting here for a while
                // we should use hysteresis, once we drop we drop completely
                if (chunk.paketstart && chunk.incomtime) {
                  if (now - chunk.incomtime > curdroptime) {
                    console.log(
                      'outgoing writer DROP, time delay',
                      type,
                      pakettime,
                      now - chunk.incomtime,
                      curdroptime
                    )
                    dropcurrentpaketwrite = true
                    curdroptime = 20
                    if (chunk.keyframe) dropuntilkeyframe = true // if we have a keyframe and drop, we must drop until the next one
                  } else {
                    curdroptime = 100 // reset
                  }
                }

                if (!dropcurrentpaketwrite && !dropuntilkeyframe) {
                  await streamwriterOut.write(chunk.paket)
                }
                paketsinwait--
                outgoingbuffer -= chunk.paket.byteLength

                if (chunk.paketend) {
                  dropcurrentpaketwrite = false
                }
              } else if (chunk.message) {
                await streamwriterOut.write(chunk.message)
              }
            } catch (error) {
              if (writefailedres) {
                console.log('DEBUG writefailed writing', error)
                writefailedres('writefailed')
                writefailedres = undefined
              }
              running = false
            }
          }
        }
        this.registerStream(curid, type, writeChunk)
        const reading = async () => {
          // eslint-disable-next-line no-unmodified-loop-condition
          while (running && streamrunning()) {
            while (parseHelper.hasMessageOrPaket()) {
              // only messages for controlling
              const message = parseHelper.getMessageOrPaket() // process them, e.g. change quality of stream
              if (message.task === 'close') {
                running = false
                return
              } else if (message.task === 'incQual') {
                const newqual = increaseQual(curqual)
                console.log('incqual', newqual, nextqual)
                if (newqual !== curqual) {
                  nextqual = newqual
                  qualchangeStor = [] // reset any already ongoing change
                }
              } else if (message.task === 'decQual') {
                const newqual = decreaseQual(curqual)
                console.log('decqual', newqual, nextqual)
                if (newqual !== curqual) {
                  nextqual = newqual
                  qualchangeStor = [] // reset any already ongoing change
                }
              } else if (message.task === 'chgId') {
                console.log('debug ChgID ticket', message)
                let messid
                let dectics
                if (message.tickets) {
                  dectics = await ticketDecode({
                    tickets: message.tickets,
                    dir: 'incoming'
                  })
                  if (dectics) {
                    messid = dectics.client
                    console.log('client info for change', dectics)
                  }
                }
                console.log('incoming change', curid, messid)

                if (curid !== messid) {
                  // we do not need the next line, writeChunk takes care about unregistering
                  // this.unregisterStream(curid, type, writeChunk)
                  // check if stream is available and not acquire it from remote
                  if (messid) {
                    console.log('change to', messid)
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
                      console.log('before acquireStreams')
                      await this.acquireStreams({
                        next: dectics.next,
                        nextspki: dectics.nextspki,
                        tickets: dectics.tickets,
                        id: messid,
                        type
                      })
                      console.log('after acquireStreams')
                    }
                    newid = messid
                    const checkqual = fixQuality(curqual)
                    if (checkqual !== curqual) nextqual = checkqual

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
        }
        await Promise.race([reading(), writefailed, writing()])
      } catch (error) {
        console.log('error processOutgoingStream', error)
      }
      outgoingpipe.flush()
      try {
        console.log('UNREGISTER STREAM SECTION REACHED')
        if (curid !== 'sleep') this.unregisterStream(curid, type, writeChunk)
        if (newid && newid !== 'sleep')
          this.unregisterStream(newid, type, writeChunk)

        streamreader.releaseLock()
        streamreader = undefined
        streamwriterOut.releaseLock()
        streamwriterOut = undefined
        /* await stream.readable.cancel()
        console.log('mark prob 10')
        await stream.writable.close()
        console.log('mark prob 11') */ // not needed
      } catch (error) {
        console.log('error cleanup processIncomingStream', error)
      }
    }

    // our stream fetcher
    const processStreamFetcher = ({ id, type, tickets, nonce }) => {
      // ok we install listeners to automatically fetch all qualities
      console.log('PROCESSTREAMFETCHER', tickets, id, type)

      const streams = {} // indexed by quality
      if (!fetchers[id]) fetchers[id] = {}
      if (fetchers[id][type]) {
        console.log('double fetcher', id, type, fetchers[id][type])
        console.log('fetcher state', fetchers)
        console.log('fetchers write', fetchersWrite[id])
        console.trace()
        throw new Error('Should not happen double fetcher')
      }
      console.log('PROCESSTREAMFETCHER mark1')
      fetchers[id][type] = streams

      const writeTest = async (chunk, pid, quality) => {
        if (!streams[quality]) {
          try {
            console.log('writeTest inspect session', session)
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

            console.log('WRITETEST BSON DEBUG', {
              command: 'configure',
              dir: 'incoming', // routers perspective
              quality,
              type,
              nonce
            })

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
                console.log('PROCESSSTREAMFETCHER finally', streams)
                console.log(
                  'PROCESSSTREAMFETCHER diag',
                  streams[quality] === stream,
                  streams[quality]
                )
                if (streams[quality] === stream) delete streams[quality]
                console.log('PROCESSSTREAMFETCHER finally after', streams)
              })
              .catch((error) =>
                console.log('fetch processOutgoing Stream problem', error)
              )
            // ok now we can install, sth that passes on the data?
          } catch (error) {
            console.log('fetch streams error:', cleanupEntered, error)
            // throw new Error('failure fetch stream:' + error) // do not break
          }
        }
      }
      console.log('PROCESSTREAMFETCHER mark2')

      this.registerStream(id, type, writeTest)
      console.log('PROCESSTREAMFETCHER mark3', id)
      if (!fetchersWrite[id]) fetchersWrite[id] = {}
      console.log('PROCESSTREAMFETCHER mark4', fetchersWrite[id])
      if (fetchersWrite[id][type]) {
        console.log('double fetcher write', id, type, fetchers[id][type])
        console.log('fetcher state', fetchers)
        throw new Error('Should not happen double fetcher write')
      }
      console.log('PROCESSTREAMFETCHER mark5', fetchersWrite[id])
      fetchersWrite[id][type] = writeTest
      console.log(
        'PROCESSTREAMFETCHER mark6',
        fetchersWrite[id],
        fetchers[id][type]
      )
    }

    const processRemoveStreamFetcher = ({ id, type }) => {
      console.log(
        'PROCESS REMOVE STREAM FETCHER',
        id,
        type,
        fetchers[id],
        fetchersWrite[id]
      )
      if (fetchers[id] && fetchers[id][type]) {
        console.log('PROCESS REMOVE STREAM FETCHER MARK 1')
        const closestreams = async () => {
          try {
            const allstreams = fetchers[id][type]
            console.log('PROCESS REMOVE DBG', fetchers[id][type])
            delete fetchers[id][type]
            if (Object.keys(fetchers[id]).length === 0) delete fetchers[id]
            for (const quality in allstreams) {
              delete allstreams[quality] // this triggers, that it is not running
            }
            // const cancels = streams.map((stream) => stream.readable.cancel())
            // const closes = streams.map((stream) => stream.writable.close())
            // await Promise.all([/* ...cancels, * ...closes])
          } catch (error) {
            console.log('processRemoveFetcher: ', error)
          }
        }
        closestreams().catch((error) => {
          console.log('Problem ProcessRemoveStreamFetcher streams:', error)
        })
      }
      if (fetchersWrite[id] && fetchersWrite[id][type]) {
        const writeTest = fetchersWrite[id][type]
        delete fetchersWrite[id][type]
        if (Object.keys(fetchersWrite[id]).length === 0)
          delete fetchersWrite[id]
        console.log('PROCESS REMOVE STREAM FETCHER MARK 2')
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
        while (running) {
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
              if (router) console.log('INCOMING ROUTER MESSAGE', message)
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
                  console.log('NONCE DEBUG pre', message.nonce, taskTickets)
                  dectics = taskTickets[message.nonce]
                  // delete taskTickets[message.nonce] // my bad, nonce is actually used multiple times for every quality, type
                }
                pspos = 10
                if (router) console.log('ROUTER DEBUG MARK 1')
                pspos = 11
                if (!dectics) {
                  console.log(
                    'NONCE DEBUG',
                    !!router,
                    message.nonce,
                    taskTickets
                  )
                  streamReader.releaseLock()
                  pspos = 12
                  stream.readable.cancel()
                  pspos = 13
                  break
                }
                if (router) console.log('ROUTER DEBUG MARK 2')

                // later may be routing code
                if (message.dir === 'incoming') {
                  pspos = 14
                  if (router) console.log('incoming from ROUTER')
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
                  if (router)
                    console.log(
                      'outgoing from ROUTER should not happen',
                      streamReader
                    )
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
                console.log('first message ignore close', message)
                streamReader.releaseLock()
                stream.readable.cancel()
                break
              } else console.log('NOP')
            } else {
              pspos = 21
              // case clientIsRouter
              console.log('client is router', message)
              console.log('CLIENT IS ROUTER')
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
                  stream.readable.cancel()
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
                console.log('STREAMFETCHER', message, dectics)
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
                console.log('STOP STREAM FETCH')
                const dectics = await ticketDecode({
                  tickets: message.tickets,
                  dir: 'outgoing'
                })
                console.log('STOP STREAM FETCH 1')
                pspos = 29
                if (!dectics) {
                  streamReader.releaseLock()
                  stream.readable.cancel()
                  break
                }
                console.log('STOP STREAM FETCH 2')
                pspos = 30
                processRemoveStreamFetcher({
                  id: dectics.client,
                  type: message.type
                })
                console.log('STOP STREAM FETCH 3')
                pspos = 31
              } else {
                console.log('unknown command close', message)
                streamReader.releaseLock()
                stream.readable.cancel()
                break
              }
              console.log('CLIENT IS ROUTER problem', message)
            }
          } else {
            pspos = 32
            garbage++
            console.log(
              'has no message or package',
              garbage,
              clientIsRouter,
              router,
              paket
            )
            if (garbage > 10) {
              console.log('reject stream with GARBAGE')
              stream.close(400)
              // or should we disconnect the whole session?
              break
            }
          }
          console.log('paket loop end')
          pspos = 33
        }
        console.log('processStream exited')
      } catch (error) {
        console.log('error in processStream', pspos, error)
      }
      // do not add anything outside the try catch
    }

    const streamProcess = async () => {
      let bidicount = 0
      // now, we process every incoming bidistream and see what it wants
      try {
        const bidiReader = session.incomingBidirectionalStreams.getReader()
        // eslint-disable-next-line no-unmodified-loop-condition
        while (running) {
          const bidistr = await bidiReader.read()
          if (bidistr.done) {
            console.log('bidiReader terminated', clientIsRouter, router)
            break
          }
          if (bidistr.value) {
            bidicount++
            console.log('incoming bidirectional stream', bidicount)
            processStream(bidistr.value).catch((error) => {
              console.log('sP problem processStream', error)
            })
          }
        }
      } catch (error) {
        console.log('bidirectional reader exited with', error)
      }
    }
    const requestProcess = async () => {
      try {
        console.log('RP 1')
        const bidistrCtrl = await session.createBidirectionalStream()
        console.log('RP 2')
        const bidiWriter = await bidistrCtrl.writable.getWriter()
        console.log('RP 3')
        const writFunc = async (chunk) => {
          try {
            await bidiWriter.write(chunk.message)
          } catch (error) {
            console.log('Bidiwriter failed:', error)
            throw new Error('Bidiwriter failed:', error)
          }
        }
        console.log('RP 4')
        // TODO authentification
        while (true) {
          const request = await requester[0]
          requester.shift()
          console.log('RP 5', request)
          if (request.close) {
            console.log('RP 6')
            try {
              await session.close({
                reason: 'close requested',
                closeCode: 501
              })
            } catch (error) {
              console.log('problem during close request:', error)
              break
            }
            break
          }
          console.log('RP 7')
          if (request.get) {
            console.log('RP 8')
            const { tickets, type, id } = request
            const nonce = this.createNonce()
            console.log('CREATED NONCE', nonce)

            taskTickets[nonce] = { client: id } // that is our ticket, already decoded
            console.log('CREATED NONCE TICKET', taskTickets[nonce], taskTickets)
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
              console.log('problem during get request:', error)
              break
            }
          }
          console.log('RP 9')
          if (request.stop) {
            console.log('RP 10 STOP')
            const { tickets, type, id /* only for debugging */ } = request
            console.log('RP 10 STOP FETCH', id)
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
              console.log('problem during stop request:', error)
              break
            }
          }
          console.log('RP 11')
          // do stuff
        }
        try {
          bidiWriter.releaseLock()
        } catch (error) {
          console.log('problem releasing Writer', error)
        }
        try {
          // bidistrCtrl.writable.close(501)
          bidistrCtrl.readable.cancel(501)
        } catch (error) {
          console.log('DEBUG BUG bidistrCtrl', bidistrCtrl)
          console.log('problem closing stream', error)
        }
      } catch (error) {
        console.log('Problem during requests:', error)
      }
    }
    try {
      if (router) await Promise.all([streamProcess(), requestProcess()])
      else await Promise.all([streamProcess()])
    } catch (error) {
      console.log('problem in streamProcess/requestProcess', error)
    }
  }
}
