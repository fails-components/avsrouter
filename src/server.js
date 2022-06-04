import { Http3Server } from '@fails-components/webtransport'
import { generateWebTransportCertificate } from './certificate.js'
import { existsSync, readFileSync, writeFile } from 'node:fs'
import { TransformStream } from 'node:stream/web'
import { AVSrouter } from './avsrouter.js'



const router = new AVSrouter()

let certificate = null

if (existsSync('./certificatecache.json')) {
  certificate = JSON.parse(
    readFileSync('./certificatecache.json', { encoding: 'utf8', flag: 'r' })
  )
}

if (!certificate) {
  const attrs = [
    { shortName: 'C', value: 'DE' },
    { shortName: 'ST', value: 'Berlin' },
    { shortName: 'L', value: 'Berlin' },
    { shortName: 'O', value: 'avsrouter Test Server' },
    { shortName: 'CN', value: '127.0.0.1' }
  ]
  certificate = await generateWebTransportCertificate(attrs, {
    days: 13
  })
  writeFile('./certificatecache.json', JSON.stringify(certificate),(err)=>{
    console.log('write certificate cache error', err)
  })
}

console.log('certificate hash ', certificate.fingerprint)
console.log(
  'certificate hash buffer',
  certificate.hash,
  new Uint8Array(certificate.hash)
)
console.log('start Server')

let http3serverv4
let http3serverv6
try {
  const secret = 'mysecretveryvery' // TODO replace with random stuff
  http3serverv4 = new Http3Server({
    port: 8081,
    host: '0.0.0.0',
    secret, 
    cert: certificate.cert, // unclear if it is the correct format
    privKey: certificate.private
  })
  /* http3serverv6 = new Http3Server({
    port: 8081,
    host: '::',
    secret, 
    cert: certificate.cert, // unclear if it is the correct format
    privKey: certificate.private
  }) */
  certificate = null
  router.runServerLoop(http3serverv4)
  // router.runServerLoop(http3serverv6)

  http3serverv4.startServer() // you can call destroy to remove the server
  console.log('server started ipv4')
 /* http3serverv6.startServer() // you can call destroy to remove the server
  console.log('server started ipv6') */
} catch (error) {
  console.log('http3error', error)
}
